from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import httpx
import os

app = FastAPI()

# ✅ CORS aktivieren (z. B. für SAP SAC)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Im Produktivbetrieb einschränken
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Supabase-Konfiguration
SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

@app.get("/")
def root():
    return {"status": "Supabase proxy with metadata is running"}

# ✅ Metadaten-Endpunkt für SAC
@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="{table_name}_schema">
      <EntityType Name="{table_name}">
        <Key>
          <PropertyRef Name="Year"/>
        </Key>
{metadata_xml_fields}
      </EntityType>
      <EntityContainer Name="{table_name}Container">
        <EntitySet Name="{table_name}" EntityType="{table_name}_schema.{table_name}"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    return Response(content=metadata_xml, media_type="application/xml")

# ✅ Daten-Endpunkt (liefert Tabelle an SAC)
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
