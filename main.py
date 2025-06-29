SUPABASE_URL = "https://prfhwrztbkewlujzastt.supabase.co/rest/v1/"
SUPABASE_API_KEY = "your-supabase-service-role-key"  # Set this in Render

...

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
        print("Supabase error:", response.status_code, response.text)
        raise HTTPException(status_code=500, detail=response.text)

    return JSONResponse(content={
        "@odata.context": f"$metadata#{table_name}",
        "value": response.json()
    })
