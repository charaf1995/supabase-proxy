from fastapi.responses import Response

@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx
  xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
  xmlns:edm="http://docs.oasis-open.org/odata/ns/edm"
  Version="4.0">

  <edmx:DataServices>
    <edm:Schema Namespace="{table_name}Model">
      <edm:EntityType Name="{table_name}">
        <edm:Key>
          <edm:PropertyRef Name="Year"/>
        </edm:Key>
        <edm:Property Name="Year" Type="Edm.Int32" Nullable="false"/>
        <edm:Property Name="Month" Type="Edm.Int32"/>
        <edm:Property Name="DayofMonth" Type="Edm.Int32"/>
        <edm:Property Name="DayOfWeek" Type="Edm.Int32"/>
        <edm:Property Name="DepTime" Type="Edm.String"/>
        <edm:Property Name="CRSDepTime" Type="Edm.String"/>
        <edm:Property Name="DepDelay" Type="Edm.String"/>
        <edm:Property Name="ArrTime" Type="Edm.String"/>
        <edm:Property Name="CRSArrTime" Type="Edm.String"/>
        <edm:Property Name="ArrDelay" Type="Edm.String"/>
        <edm:Property Name="UniqueCarrier" Type="Edm.String"/>
        <edm:Property Name="FlightNum" Type="Edm.String"/>
        <edm:Property Name="TailNum" Type="Edm.String"/>
        <edm:Property Name="ActualElapsedTime" Type="Edm.String"/>
        <edm:Property Name="CRSElapsedTime" Type="Edm.String"/>
        <edm:Property Name="AirTime" Type="Edm.String"/>
        <edm:Property Name="Origin" Type="Edm.String"/>
        <edm:Property Name="Dest" Type="Edm.String"/>
        <edm:Property Name="Distance" Type="Edm.Double"/>
        <edm:Property Name="TaxiIn" Type="Edm.String"/>
        <edm:Property Name="TaxiOut" Type="Edm.String"/>
        <edm:Property Name="Cancelled" Type="Edm.String"/>
        <edm:Property Name="CancellationCode" Type="Edm.String"/>
        <edm:Property Name="Diverted" Type="Edm.String"/>
        <edm:Property Name="CarrierDelay" Type="Edm.String"/>
        <edm:Property Name="WeatherDelay" Type="Edm.String"/>
        <edm:Property Name="NASDelay" Type="Edm.String"/>
        <edm:Property Name="SecurityDelay" Type="Edm.String"/>
        <edm:Property Name="LateAircraftDelay" Type="Edm.String"/>
        <edm:Property Name="Column1" Type="Edm.String"/>
      </edm:EntityType>

      <edm:EntityContainer Name="Container">
        <edm:EntitySet Name="{table_name}" EntityType="{table_name}Model.{table_name}"/>
      </edm:EntityContainer>
    </edm:Schema>
  </edmx:DataServices>
</edmx:Edmx>'''
    return Response(content=metadata_xml.strip(), media_type="application/xml")
