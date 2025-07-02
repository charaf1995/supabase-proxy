from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os

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

@app.get("/")
def root():
    return {"status": "Supabase OData proxy is running"}

# ✅ $metadata – SAP-kompatibel
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
  <edmx:DataServices>
    <Schema xmlns="http://schemas.microsoft.com/ado/2008/09/edm" Namespace="{table_name}_schema">
      <EntityType Name="{table_name}">
        <Key><PropertyRef Name="Year" /></Key>
        <Property Name="Year" Type="Edm.Int64" Nullable="false" />
        <Property Name="Month" Type="Edm.Int64" />
        <Property Name="DayofMonth" Type="Edm.Int64" />
        <Property Name="DayOfWeek" Type="Edm.Int64" />
        <Property Name="DepTime" Type="Edm.String" />
        <Property Name="CRSDepTime" Type="Edm.Int64" />
        <Property Name="ArrTime" Type="Edm.String" />
        <Property Name="CRSArrTime" Type="Edm.Int64" />
        <Property Name="UniqueCarrier" Type="Edm.String" />
        <Property Name="FlightNum" Type="Edm.Int64" />
        <Property Name="TailNum" Type="Edm.String" />
        <Property Name="ActualElapsedTime" Type="Edm.String" />
        <Property Name="CRSElapsedTime" Type="Edm.Int64" />
        <Property Name="AirTime" Type="Edm.String" />
        <Property Name="ArrDelay" Type="Edm.String" />
        <Property Name="DepDelay" Type="Edm.String" />
        <Property Name="Origin" Type="Edm.String" />
        <Property Name="Dest" Type="Edm.String" />
        <Property Name="Distance" Type="Edm.Int64" />
        <Property Name="TaxiIn" Type="Edm.String" />
        <Property Name="TaxiOut" Type="Edm.String" />
        <Property Name="Cancelled" Type="Edm.String" />
        <Property Name="CancellationCode" Type="Edm.String" />
        <Property Name="Diverted" Type="Edm.Boolean" />
        <Property Name="CarrierDelay" Type="Edm.String" />
        <Property Name="WeatherDelay" Type="Edm.String" />
        <Property Name="NASDelay" Type="Edm.String" />
        <Property Name="SecurityDelay" Type="Edm.String" />
        <Property Name="LateAircraftDelay" Type="Edm.String" />
      </EntityType>
      <EntityContainer Name="{table_name}Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}_schema.{table_name}" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml.strip(), media_type="application/xml")

# ✅ Haupt-OData-Endpunkt – mit Feldnamen-Fix
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
        raise HTTPException(status_code=500, detail=response.text)

    raw_data = response.json()

    # 🔁 Korrigiere alle Feldnamen: z. B. "origin" → "Origin"
    fixed_data = []
    for row in raw_data:
        fixed_row = {}
        for key, value in row.items():
            fixed_key = key[0].upper() + key[1:] if len(key) > 0 else key
            fixed_row[fixed_key] = value
        fixed_data.append(fixed_row)

    return JSONResponse(
        content={
            "@odata.context": f"$metadata#{table_name}",
            "value": fixed_data
        },
        media_type="application/json"
    )
