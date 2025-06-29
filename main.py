from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware  # ✅ Added for CORS
import httpx
from fastapi.responses import JSONResponse
import os

app = FastAPI()

# ✅ Add CORS middleware (required by SAC)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to SAP SAC domains later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()

# ✅ Supabase base URL (no table name)
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"

# ✅ API key is set via Render environment variable
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# ✅ Allowed login credentials (Basic Auth)
ALLOWED_USERS = {
    "sap_user": "sap_password"
}

# ✅ Auth check
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if ALLOWED_USERS.get(credentials.username) != credentials.password:
        raise HTTPException(status_code=403, detail="Invalid credentials")
    return credentials.username

# ✅ Optional root endpoint
@app.get("/")
def root():
    return {"status": "FastAPI Supabase Proxy is running"}

# ✅ Main proxy endpoint for SAC
@app.get("/odata/{table_name}")
async def proxy_odata(
    table_name: str,
    request: Request,
    username: str = Depends(verify_credentials)
):
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
        print("Supabase error:", response.status_code, response.text)
        raise HTTPException(status_code=500, detail=response.text)

    return JSONResponse(content={
        "@odata.context": f"$metadata#{table_name}",
        "value": response.json()
    })
