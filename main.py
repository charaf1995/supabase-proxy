from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os

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

# ✅ Metadata endpoint for SAC compatibility
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="{table_name}Model">
      <EntityType Name="{table_name}">
        <Key><PropertyRef Name="Year"/></Key>
        <Property Name="Year" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Month" Type="Edm.Int32"/>
        <Property Name="DayofMonth" Type="Edm.Int32"/>
        <Property Name="DayOfWeek" Type="Edm.Int32"/>
        <Property Name="DepTime" Type="Edm.String"/>
        <Property Name="CRSDepTime" Type="Edm.String"/>
        <Property Name="DepDelay" Type="Edm.String"/>
        <Property Name="ArrTime" Type="Edm.String"/>
        <Property Name="CRSArrTime" Type="Edm.String"/>
        <Property Name="ArrDelay" Type="Edm.String"/>
        <Property Name="UniqueCarrier" Type="Edm.String"/>
        <Property Name="FlightNum" Type="Edm.String"/>
        <Property Name="TailNum" Type="Edm.String"/>
        <Property Name="ActualElapsedTime" Type="Edm.String"/>
        <Property Name="CRSElapsedTime" Type="Edm.String"/>
        <Property Name="AirTime" Type="Edm.String"/>
        <Property Name="Origin" Type="Edm.String"/>
        <Property Name="Dest" Type="Edm.String"/>
        <Property Name="Distance" Type="Edm.Double"/>
        <Property Name="TaxiIn" Type="Edm.String"/>
        <Property Name="TaxiOut" Type="Edm.String"/>
        <Property Name="Cancelled" Type="Edm.String"/>
        <Property Name="CancellationCode" Type="Edm.String"/>
        <Property Name="Diverted" Type="Edm.String"/>
        <Property Name="CarrierDelay" Type="Edm.String"/>
        <Property Name="WeatherDelay" Type="Edm.String"/>
        <Property Name="NASDelay" Type="Edm.String"/>
        <Property Name="SecurityDelay" Type="Edm.String"/>
        <Property Name="LateAircraftDelay" Type="Edm.String"/>
        <Property Name="Column1" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}Model.{table_name}"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>'''
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Main proxy endpoint for SAC
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

    # ✅ Now correctly indented
    supabase_data = response.json()

    if isinstance(supabase_data, list):
        return JSONResponse(
            content={
                "@odata.context": f"$metadata#{table_name}",
                "value": supabase_data
            },
            media_type="application/json"
        )
    else:
        raise HTTPException(status_code=500, detail="Supabase returned invalid data format")
