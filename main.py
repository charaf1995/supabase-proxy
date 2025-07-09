from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from urllib.parse import parse_qs, urlencode
import httpx
import os
from email.parser import BytesParser
from email.policy import default as default_policy

app = FastAPI()

# ✅ CORS for SAP SAC / DataSphere
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase connection
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
if not SUPABASE_API_KEY:
    raise RuntimeError("SUPABASE_API_KEY is not set in environment variables.")

@app.get("/")
def root():
    return {"status": "Supabase OData proxy (optimized) is running"}

# ✅ $metadata – SAP compatible
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
        <!-- Add your other fields here -->
      </EntityType>
      <EntityContainer Name="{table_name}Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}_schema.{table_name}" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Main OData endpoint (optimized)
@app.get("/odata/{table_name}")
async def proxy_odata(table_name: str, request: Request):
    raw_query = request.url.query
    params = parse_qs(raw_query)

    # Auto-convert $select → select
    if '$select' in params:
        params['select'] = params.pop('$select')

    # Simple $filter conversion (only for "eq")
    if '$filter' in params:
        filter_value = params.pop('$filter')[0]
        if ' eq ' in filter_value:
            column, value = filter_value.split(' eq ')
            params[column] = f"eq.{value}"

    # Build final query string
    query_string = urlencode(params, doseq=True)

    full_url = f"{SUPABASE_URL}{table_name}"
    if query_string:
        full_url += f"?{query_string}"

    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Accept": "application/json"
    }

    async with httpx.AsyncClient(timeout=10) as client:  # 10 sec timeout
        response = await client.get(full_url, headers=headers)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=response.text)

    raw_data = response.json()

    # ✅ Fast field name fix (optimized)
    fixed_data = [
        {key.capitalize(): value for key, value in row.items()}
        for row in raw_data
    ]

    return JSONResponse(
        content={
            "@odata.context": f"$metadata#{table_name}",
            "value": fixed_data
        },
        media_type="application/json"
    )

# ✅ Batch endpoint (optimized)
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

        # Direct function call (no HTTP request overhead)
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
