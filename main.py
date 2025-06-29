from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import httpx
from fastapi.responses import JSONResponse
import os

app = FastAPI()  # ✅ This defines the FastAPI app
security = HTTPBasic()

# ✅ Supabase project base URL (without table name)
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"

# ✅ API key will be securely set in Render environment settings
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# ✅ Allowed login credentials (SAC users)
ALLOWED_USERS = {
    "sap_user": "sap_password"
}

# ✅ Basic Auth validator
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if ALLOWED_USERS.get(credentials.username) != credentials.password:
        raise HTTPException(status_code=403, detail="Invalid credentials")
    return credentials.username

# ✅ Root endpoint (optional)
@app.get("/")
def root():
    return {"status": "FastAPI Supabase Proxy is running"}

# ✅ OData-style proxy endpoint
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
        print("Supabase error:", response.status_code, response.text)  # visible in Render logs
        raise HTTPException(status_code=500, detail=response.text)

    return JSONResponse(content={
        "@odata.context": f"$metadata#{table_name}",
        "value": response.json()
    })
