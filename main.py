from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os
from email.parser import BytesParser
from email.policy import default as default_policy

app = FastAPI()

# ✅ CORS für SAP Datasphere & SAC
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
if not SUPABASE_API_KEY:
    raise RuntimeError("SUPABASE_API_KEY is not set")

@app.get("/")
def root():
    return {"status": "OData v4 Proxy with Supabase for SAP Datasphere is running"}

# ✅ OData v4 $metadata Endpoint (SAP Datasphere kompatibel)
@app.get("/odata/Flights/$metadata")
def metadata():
    metadata_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="FlightsService">
      <EntityType Name="Flight">
        <Key>
          <PropertyRef Name="Id" />
        </Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false" />
        <!-- Weitere Felder hier -->
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Flights" EntityType="FlightsService.Flight" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ OData v4 GET Endpoint (Supabase Proxy)
@app.get("/odata/Flights")
async def get_flights(request: Request):
    query_string = request.url.query
    full_url = f"{SUPABASE_URL}flights"
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

    return JSONResponse(
        content={
            "@odata.context": "$metadata#Flights",
            "value": response.json()
        },
        media_type="application/json"
    )

# ✅ OData v4 $batch Endpoint (GET-only, SAP Datasphere-kompatibel)
@app.post("/odata/Flights/$batch")
async def batch_handler(request: Request):
    content_type = request.headers.get("Content-Type", "")
    if "multipart/mixed" not in content_type:
        raise HTTPException(status_code=400, detail="Invalid Content-Type for Batch")

    boundary = content_type.split("boundary=")[-1]
    body = await request.body()

    parser = BytesParser(policy=default_policy)
    msg = parser.parsebytes(
        b"Content-Type: " + content_type.encode() + b"\n\n" + body
    )

    batch_parts = []
    for part in msg.iter_parts():
        part_body = part.get_content()
        lines = part_body.splitlines()
        if not lines:
            continue

        request_line = lines[0]
        method, path, _ = request_line.split()
        if method != "GET":
            raise HTTPException(status_code=400, detail="Only GET supported in Batch")

        query = path.split("?", 1)[1] if "?" in path else ""

        req = Request({
            "type": "http",
            "method": "GET",
            "query_string": query.encode(),
            "headers": [],
            "path": path.split("?")[0],
        }, receive=None)

        response = await get_flights(req)
        response_body = response.body.decode()

        part_response = (
            "Content-Type: application/http\n"
            "Content-Transfer-Encoding: binary\n\n"
            "HTTP/1.1 200 OK\n"
            "Content-Type: application/json\n\n"
            f"{response_body}\n"
        )
        batch_parts.append(part_response)

    response_boundary = "batch_response_boundary"
    batch_response = ""
    for part in batch_parts:
        batch_response += f"--{response_boundary}\n{part}"
    batch_response += f"--{response_boundary}--"

    return Response(
        content=batch_response,
        media_type=f"multipart/mixed; boundary={response_boundary}"
    )
