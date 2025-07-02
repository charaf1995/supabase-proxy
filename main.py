from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os

app = FastAPI()

# ✅ Allow SAC or any domain (restrict in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can change this to specific SAC domains later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase configuration
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")  # Set this in Render environment

# ✅ Root endpoint
@app.get("/")
def root():
    return {"status": "Supabase proxy is running"}

# ✅ OData metadata endpoint (needed for SAC to show fields)
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    # Replace the fields below with your actual Supabase table columns and types
    metadata_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="{table_name}_schema">
      <EntityType Name="{table_name}">
        <Key>
          <PropertyRef Name="id"/>
        </Key>
        <Property Name="id" Type="Edm.Int32"/>
        <Property Name="departure" Type="Edm.String"/>
        <Property Name="arrival" Type="Edm.String"/>
        <Property Name="delay" Type="Edm.Int32"/>
        <!-- ➕ Add more <Property> elements here if your table has more columns -->
      </EntityType>
      <EntityContainer Name="{table_name}Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}_schema.{table_name}"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml, media_type="application/xml")

# ✅ Main proxy endpoint (data from Supabase to SAC)
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
        print("❌ Supabase error:", response.status_code, response.text)
        raise HTTPException(status_code=500, detail=response.text)

    return JSONResponse(
        content={
            "@odata.context": f"$metadata#{table_name}",
            "value": response.json()
        },
        media_type="application/json"
    )
