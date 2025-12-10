import json
import os
from typing import Iterable, Optional, Tuple
from urllib.parse import urljoin

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from shapely.errors import ShapelyError
from shapely.geometry import mapping, shape

# Internal URLs for pygeoapi and PostgREST services
PYGEOAPI_URL = os.getenv("PYGEOAPI_INTERNAL")
POSTGREST_URL = os.getenv("POSTGREST_INTERNAL")

# FastAPI app setup
app = FastAPI(title="infDB API Gateway", version="1.0.0")
app.add_middleware(GZipMiddleware, minimum_size=500)  # Enable gzip compression for large responses


# Root endpoint for basic API status
@app.get("/")
async def root():
    return {"message": "INFDB API is running."}


# Health check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}


# Health check for PostgREST service
@app.get("/postgrest/health")
async def postgrest_health():
    timeout = httpx.Timeout(5.0, read=5.0)
    url = POSTGREST_URL
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url)
        return {"ok": r.status_code < 400, "status_code": r.status_code}
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"PostgREST unreachable at {url}: {e}") from e


# Helper to proxy HTTP responses, preserving headers except for hop-by-hop headers
def _proxy_response(resp: httpx.Response) -> Response:
    media = resp.headers.get("content-type", "application/json")
    r = Response(content=resp.content, status_code=resp.status_code, media_type=media)
    hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-encoding",
    }
    for k, v in resp.headers.items():
        lk = k.lower()
        if lk not in hop and lk not in {"content-length", "content-type"}:
            r.headers[k] = v
    return r


# Helper to proxy requests to another service
async def _proxy(
    req: Request, base_url: str, subpath: str, *, override_params: Optional[Iterable[Tuple[str, str]]] = None
) -> httpx.Response:
    method = req.method
    target = urljoin(base_url, subpath)
    body = await req.body()
    headers = {k: v for k, v in req.headers.items() if k.lower() != "host"}
    timeout = httpx.Timeout(30.0, read=60.0)
    params = list(override_params) if override_params is not None else dict(req.query_params)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method, target, params=params, content=body, headers=headers)
    return resp


# ---- PostgREST Endpoints ----


# GET endpoint to fetch data from PostgREST, with geometry simplification
@app.get("/postgrest/{schema}/{table}")
async def get_postgrest(
    request: Request,
    schema: str,
    table: str,
    limit: int = 100,
    tolerance: float = Query(100, description="Geometry simplification tolerance (units match your data)"),
):
    # Only pass allowed params to PostgREST, filter out internal params
    passthrough = [
        (k, v)
        for k, v in request.query_params.multi_items()
        if k not in {"schema", "table", "limit", "offset", "tolerance"}
    ]
    passthrough.append(("limit", str(limit)))
    passthrough.append(("offset", str(0)))

    headers = dict(request.headers)
    headers["Accept-Profile"] = schema  # Specify schema for PostgREST

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(urljoin(POSTGREST_URL, table), params=passthrough, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Cannot reach PostgREST at {POSTGREST_URL} -> {table}: {e.__class__.__name__}: {e}",
        ) from e

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    # Simplify geometry in the response if present
    def _simplify_geometry(obj, tolerance=100):
        for key in ["geometry", "geom"]:
            if key in obj and isinstance(obj[key], dict) and "coordinates" in obj[key]:
                try:
                    geom = shape(obj[key])
                    simple_geom = geom.simplify(tolerance, preserve_topology=True)
                    obj[key] = mapping(simple_geom)
                except ShapelyError:
                    pass
        return obj

    if resp.headers.get("content-type", "").startswith("application/json"):
        data = resp.json()
        if isinstance(data, list):
            data = [_simplify_geometry(item, tolerance=tolerance) for item in data]
        elif isinstance(data, dict):
            data = _simplify_geometry(data, tolerance=tolerance)
        return Response(content=json.dumps(data), status_code=resp.status_code, media_type="application/json")
    else:
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


# POST endpoint to insert a new row into a table via PostgREST
@app.post("/postgrest/{schema}/{table}")
async def post_postgrest(schema: str, table: str, row: dict):
    headers = {"Content-Type": "application/json", "Content-Profile": schema}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(urljoin(POSTGREST_URL, table), json=row, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Cannot reach PostgREST at {POSTGREST_URL}: {e.__class__.__name__}: {e}"
        ) from e
    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail=f"PostgREST error: {resp.text}")
    if resp.content:
        return resp.json()
    else:
        return {"status": "success"}


# PUT endpoint to update an existing row in a table via PostgREST
@app.put("/postgrest/{schema}/{table}/{item_id}")
async def put_postgrest(
    schema: str,
    table: str,
    item_id: str,
    row: dict,
    key_column: str = Query("id", description="Primary key column name"),
):
    headers = {"Content-Type": "application/json", "Content-Profile": schema}
    params = {key_column: f"eq.{item_id}"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.patch(urljoin(POSTGREST_URL, table), params=params, json=row, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Cannot reach PostgREST at {POSTGREST_URL}: {e.__class__.__name__}: {e}"
        ) from e
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=resp.status_code, detail=f"PostgREST error: {resp.text}")
    if resp.content:
        return resp.json()
    else:
        return {"status": "updated"}


# DELETE endpoint to remove a row from a table via PostgREST
@app.delete("/postgrest/{schema}/{table}/{item_id}")
async def delete_postgrest(
    schema: str, table: str, item_id: str, key_column: str = Query("id", description="Primary key column name")
):
    headers = {"Content-Type": "application/json", "Content-Profile": schema}
    params = {key_column: f"eq.{item_id}"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.delete(urljoin(POSTGREST_URL, table), params=params, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Cannot reach PostgREST at {POSTGREST_URL}: {e.__class__.__name__}: {e}"
        ) from e
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=resp.status_code, detail=f"PostgREST error: {resp.text}")
    return {"status": "deleted"}
