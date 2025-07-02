from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from fastapi.responses import Response

app = FastAPI()

# ✅ Allow all origins (can be restricted to SAC domains)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with SAC domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase config
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# ✅ Root check
@app.get("/")
def root():
    return {"status": "Supabase proxy is running"}

# ✅ Metadata endpoint (dummy, SAC compatibility)


@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="{table_name}Model">
      <EntityType Name="{table_name}">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.Int32" Nullable="false"/>
        <!-- Add more fields if you want -->
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}Model.{table_name}"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>'''
    return Response(content=metadata_xml.strip(), media_type="application/xml")


# ✅ Main proxy endpoint
@app.get("/odata/{table_name}")
async def proxy_odata(table_name: str, request: Request):
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

    return JSONResponse(
        content={
            "@odata.context": f"$metadata#{table_name}",
            "value": response.json()
        },
        media_type="application/json"
    )
