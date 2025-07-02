from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os

app = FastAPI()

# ✅ Enable CORS (for SAP SAC)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to SAC domains later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase config
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")  # Set in Render environment

# ✅ Root check
@app.get("/")
def root():
    return {"status": "Supabase proxy is running"}

# ✅ OData metadata endpoint (needed by SAC)
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    # Static metadata for the 'flights' table from your CSV
    metadata_xml = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="flights_schema">
      <EntityType Name="flights">
        <Key>
          <PropertyRef Name="Year"/>
        </Key>
        <Property Name="Year" Type="Edm.Double"/>
        <Property Name="Month" Type="Edm.Double"/>
        <Property Name="DayofMonth" Type="Edm.Double"/>
        <Property Name="DayOfWeek" Type="Edm.Double"/>
        <Property Name="DepTime" Type="Edm.Double"/>
        <Property Name="CRSDepTime" Type="Edm.Double"/>
        <Property Name="DepDelay" Type="Edm.String"/>
        <Property Name="ArrTime" Type="Edm.Double"/>
        <Property Name="CRSArrTime" Type="Edm.Double"/>
        <Property Name="ArrDelay" Type="Edm.Double"/>
        <Property Name="UniqueCarrier" Type="Edm.String"/>
        <Property Name="FlightNum" Type="Edm.Double"/>
        <Property Name="TailNum" Type="Edm.String"/>
        <Property Name="ActualElapsedTime" Type="Edm.Double"/>
        <Property Name="CRSElapsedTime" Type="Edm.Double"/>
        <Property Name="AirTime" Type="Edm.Double"/>
        <Property Name="Origin" Type="Edm.String"/>
        <Property Name="Dest" Type="Edm.String"/>
        <Property Name="Distance" Type="Edm.Double"/>
        <Property Name="TaxiIn" Type="Edm.Double"/>
        <Property Name="TaxiOut" Type="Edm.Double"/>
        <Property Name="Cancelled" Type="Edm.String"/>
        <Property Name="CancellationCode" Type="Edm.Double"/>
        <Property Name="Diverted" Type="Edm.Double"/>
        <Property Name="CarrierDelay" Type="Edm.Double"/>
        <Property Name="WeatherDelay" Type="Edm.Double"/>
        <Property Name="NASDelay" Type="Edm.Double"/>
        <Property Name="SecurityDelay" Type="Edm.Double"/>
        <Property Name="LateAircraftDelay" Type="Edm.Double"/>
        <Property Name="Column1" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="flightsContainer">
        <EntitySet Name="flights" EntityType="flights_schema.flights"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml, media_type="application/xml")

# ✅ Main data endpoint
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
