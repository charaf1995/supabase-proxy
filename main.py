from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import httpx
import os
from email.parser import BytesParser
from email.policy import default as default_policy
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# ✅ CORS für SAP SAC
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

# ✅ Basic Auth (Username/Passwort)
security = HTTPBasic()
BASIC_AUTH_USERNAME = os.getenv("user")
BASIC_AUTH_PASSWORD = os.getenv("sapsap")

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if (
        credentials.username != BASIC_AUTH_USERNAME
        or credentials.password != BASIC_AUTH_PASSWORD
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

@app.get("/")
def root():
    return {"status": "Supabase OData proxy with batch and Basic Auth is running"}

# ✅ $metadata – SAP-kompatibel, mit Auth
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    metadata_xml = """<?xml ...>"""  # (dein vorhandenes Metadata XML bleibt hier gleich)
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Haupt-OData-Endpunkt – mit Auth
@app.get("/odata/{table_name}")
async def proxy_odata(table_name: str, request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    query_string = request.url.query
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

    fixed_data = []
    for row in raw_data:
        fixed_row = {key[0].upper() + key[1:] if key else key: value for key, value in row.items()}
        fixed_data.append(fixed_row)

    return JSONResponse(
        content={"@odata.context": f"$metadata#{table_name}", "value": fixed_data},
        media_type="application/json"
    )

# ✅ $batch-Endpunkt mit Auth
@app.post("/odata/{table_name}/$batch")
async def batch_handler(table_name: str, request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    content_type = request.headers.get("Content-Type", "")
    if "multipart/mixed" not in content_type:
        raise HTTPException(status_code=400, detail="Invalid Content-Type")

    boundary = content_type.split("boundary=")[-1]
    body = await request.body()

    parser = BytesParser(policy=default_policy)
    msg = parser.parsebytes(b"Content-Type: " + content_type.encode() + b"\n\n" + body)
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
