from email.parser import BytesParser
from email.policy import default as default_policy
from fastapi.responses import Response
from starlette.datastructures import MutableHeaders

@app.post("/odata/{table_name}/$batch")
async def batch_handler(table_name: str, request: Request):
    # Hole Content-Type + Boundary
    content_type = request.headers.get("Content-Type", "")
    if "multipart/mixed" not in content_type:
        raise HTTPException(status_code=400, detail="Invalid Content-Type for Batch")

    boundary = content_type.split("boundary=")[-1]
    body = await request.body()

    # Parse Multipart/Mixed Body (Email-Parser nutzen)
    parser = BytesParser(policy=default_policy)
    msg = parser.parsebytes(
        b"Content-Type: " + content_type.encode() + b"\n\n" + body
    )

    batch_parts = []
    for part in msg.iter_parts():
        part_body = part.get_content()
        lines = part_body.splitlines()
        if not lines:
            continue

        # Parse HTTP Request in Batch (nur GET erlaubt)
        request_line = lines[0]
        method, path, _ = request_line.split()
        if method != "GET":
            raise HTTPException(status_code=400, detail="Only GET supported in Batch")

        # Query extrahieren
        query = path.split("?", 1)[1] if "?" in path else ""

        # Lokaler Aufruf deines /odata/{table_name}-Endpoints
        req = Request({
            "type": "http",
            "method": "GET",
            "query_string": query.encode(),
            "headers": [],
            "path": path.split("?")[0],
        }, receive=None)

        response = await proxy_odata(table_name, req)
        response_body = response.body.decode()

        # Multipart-Teil für Antwort aufbauen
        part_response = (
            "Content-Type: application/http\n"
            "Content-Transfer-Encoding: binary\n\n"
            "HTTP/1.1 200 OK\n"
            "Content-Type: application/json\n\n"
            f"{response_body}\n"
        )
        batch_parts.append(part_response)

    # Baue vollständige Multipart/mixed Antwort
    response_boundary = "batch_response_boundary"
    batch_response = ""
    for part in batch_parts:
        batch_response += f"--{response_boundary}\n{part}"
    batch_response += f"--{response_boundary}--"

    return Response(
        content=batch_response,
        media_type=f"multipart/mixed; boundary={response_boundary}"
    )
