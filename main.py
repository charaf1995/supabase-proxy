from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os
from email.parser import BytesParser
from email.policy import default as default_policy

app = FastAPI()

# ✅ Allow CORS (adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase Config
SUPABASE_URL = "https://your-project.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
if not SUPABASE_API_KEY:
    raise RuntimeError("SUPABASE_API_KEY is not set")

@app.get("/")
def root():
    return {"status": "Dynamic Supabase OData v4 Proxy is running"}

# ✅ Dynamic $metadata (Minimal Example, Auto Table)
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    safe_table = table_name.lower()
    metadata_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="{safe_table}_schema">
      <EntityType Name="{safe_table}">
        <Key><PropertyRef Name="id" /></Key>
        <Property Name="id" Type="Edm.Int32" Nullable="false" />
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="{safe_table}" EntityType="{safe_table}_schema.{safe_table}" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Universal Data Proxy (Dynamic Table)
@app.get("/odata/{table_name}")
async def proxy_odata(table_name: str, request: Request):
    safe_table = table_name.lower()
    query_string = request.url.query
    full_url = f"{SUPABASE_URL}{safe_table}"
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
            "@odata.context": f"$metadata#{safe_table}",
            "value": response.json()
        },
        media_type="application/json"
    )

# ✅ Universal $batch (GET-only, Dynamic Tables)
@app.post("/odata/{table_name}/$batch")
async def batch_handler(table_name: str, request: Request):
    safe_table = table_name.lower()
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

        response = await proxy_odata(safe_table, req)
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
