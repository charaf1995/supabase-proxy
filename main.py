from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import os

app = FastAPI()

# ✅ Enable CORS (important for SAC access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Optional: restrict to SAC domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase base URL (no table name)
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"

# ✅ API key from environment
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# ✅ Optional root endpoint
@app.get("/")
def root():
    return {"status": "FastAPI Supabase Proxy is running (no auth)"}

# ✅ Main proxy endpoint (open access)
@app.get("/odata/{table_name}")
async def proxy_odata(
    table_name: str,
    request: Request
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
