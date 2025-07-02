from fastapi.responses import Response

@app.get("/odata/{table_name}/$metadata")
def metadata(table_name: str):
    metadata_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="{table_name}Model"
            xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="{table_name}">
        <Key>
          <PropertyRef Name="Year"/>
        </Key>
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
