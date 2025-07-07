from email.parser import BytesParser
from email.policy import default as default_policy

@app.post("/odata/{table_name}/$batch")
async def batch_handler(table_name: str, request: Request):
    content_type = request.headers.get("Content-Type", "")
    if "multipart/mixed" not in content_type:
        raise HTTPException(status_code=400, detail="Invalid Content-Type")

    boundary = content_type.split("boundary=")[-1]
    body = await request.body()

    # Parse multipart/mixed body
    parser = BytesParser(policy=default_policy)
    msg = parser.parsebytes(
        b"Content-Type: " + content_type.encode() + b"\n\n" + body
    )

    responses = []
    for part in msg.iter_parts():
        part_body = part.get_content()
        # Parse internal HTTP request in batch
        lines = part_body.splitlines()
        request_line = lines[0]
        method, path, _ = request_line.split()
        if method != "GET":
            raise HTTPException(status_code=400, detail="Only GET supported in batch")

        # Nur lokale GET-Requests aufrufen
        query = path.split("?", 1)[1] if "?" in path else ""
        req = Request({
            "type": "http",
            "method": "GET",
            "query_string": query.encode(),
            "headers": [],
            "path": path.split("?")[0],
        }, receive=None)

        # Interner Aufruf deines Proxy-Endpunkts (z. B. /odata/Flights)
        response = await proxy_odata(table_name, req)
        responses.append({
            "status_code": 200,
            "body": response.body.decode()
        })

    # Baue Batch-Response
    batch_response = ""
    for i, resp in enumerate(responses):
        batch_response += f"--batch_response_boundary\n"
        batch_response += "Content-Type: application/http\n"
        batch_response += "Content-Transfer-Encoding: binary\n\n"
        batch_response += f"HTTP/1.1 {resp['status_code']} OK\n"
        batch_response += "Content-Type: application/json\n\n"
        batch_response += f"{resp['body']}\n"

    batch_response += "--batch_response_boundary--"

    return Response(
        content=batch_response,
        media_type="multipart/mixed; boundary=batch_response_boundary"
    )
