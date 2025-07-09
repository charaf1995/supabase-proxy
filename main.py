from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from urllib.parse import parse_qs, urlencode
import httpx
import os
from email.parser import BytesParser
from email.policy import default as default_policy

app = FastAPI()

# ✅ CORS für SAP SAC / DataSphere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase-Verbindung
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
if not SUPABASE_API_KEY:
    raise RuntimeError("SUPABASE_API_KEY is not set in environment variables.")

@app.get("/")
def root():
    return {"status": "Supabase OData proxy with batch and OData mapping is running"}

# ✅ $metadata – SAP-kompatibel
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
  <edmx:DataServices>
    <Schema xmlns="http://schemas.microsoft.com/ado/2008/09/edm" Namespace="{table_name}_schema">
      <EntityType Name="{table_name}">
        <Key><PropertyRef Name="Year" /></Key>
        <Property Name="Year" Type="Edm.Int64" Nullable="false" />
        <Property Name="Month" Type="Edm.Int64" />
        <!-- (Rest der Spalten bleibt gleich – du kannst sie ergänzen) -->
      </EntityType>
      <EntityContainer Name="{table_name}Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}_schema.{table_name}" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Haupt-OData-Endpunkt mit Query-Konverter
@app.get("/odata/{table_name}")
async def proxy_odata(table_name: str, request: Request):
    raw_query = request.url.query
    params = parse_qs(raw_query)

    # $select → select
    if '$select' in params:
        params['select'] = params.pop('$select')

    # Optional: Einfaches $filter Mapping (nur = Filter)
    if '$filter' in params:
        filter_value = params.pop('$filter')[0]
        # Beispiel: $filter=Year eq 2024 → Year=eq.2024
        if ' eq ' in filter_value:
            column, value = filter_value.split(' eq ')
            params[column] = f"eq.{value}"

    # Query neu bauen:
    query_string = urlencode(params, doseq=True)

    full_url = f"{SUPABASE_URL}{table_name}"
    if query_string:
        full_url += f"?{query_string}"

    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Accept": "application/json"
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(full_url, headers=headers)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=response.text)

    raw_data = response.json()

    # 🔁 Feldnamen korrigieren: z. B. "origin" → "Origin"
    fixed_data = []
    for row in raw_data:
        fixed_row = {key[0].upper() + key[1:] if key else key: value for key, value in row.items()}
        fixed_data.append(fixed_row)

    return JSONResponse(
        content={
            "@odata.context": f"$metadata#{table_name}",
            "value": fixed_data
        },
        media_type="application/json"
    )

# ✅ $batch-Endpunkt (SAP-kompatibel)
@app.post("/odata/{table_name}/$batch")
async def batch_handler(table_name: str, request: Request):
    content_type = request.headers.get("Content-Type", "")
    if "multipart/mixed" not in content_type:
        raise HTTPException(status_code=400, detail="Invalid Content-Type")

    boundary = content_type.split("boundary=")[-1]
    body = await request.body()

    parser = BytesParser(policy=default_policy)
    msg = parser.parsebytes(
        b"Content-Type: " + content_type.encode() + b"\n\n" + body
    )
    responses = []
    for part in msg.iter_parts():
        part_body = part.get_content()
        lines = part_body.splitlines()
        request_line = lines[0]
        method, path, _ = request_line.split()
        if method != "GET":
            raise HTTPException(status_code=400, detail="Only GET supported in batch")

        query = path.split("?", 1)[1] if "?" in path else ""
        req = Request({
            "type": "http",
            "method": "GET",
            "query_string": query.encode(),
            "headers": [],
            "path": path.split("?")[0],
        }, receive=None)

        response = await proxy_odata(table_name, req)
        responses.append({
            "status_code": 200,
            "body": response.body.decode()
        })

    batch_response = ""
    for resp in responses:
        batch_response += f"--batch_response_boundary\n"
        batch_response += "Content-Type: application/http\n"
        batch_response += "Content-Transfer-Encoding: binary\n\n"
        batch_response += f"HTTP/1.1 {resp['status_code']} OK\n"
        batch_response += "Content-Type: application/json\n\n"
        batch_response += f"{resp['body']}\n"
    batch_response += "--batch_response_boundary--"
    return Response(
        content=batch_response,
        media_type="multipart/mixed; boundary=batch_response_boundary"
    )
