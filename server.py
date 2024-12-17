import json

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Mount the static directory
app.mount("/static", StaticFiles(directory="static"), name="static")
data_collection_dir = "Data/API/"


@app.get("/", response_class=HTMLResponse)
async def serve_html():
    html_path = Path("templates/index.html")  # Adjust the path if your file is named differently or in another directory
    if html_path.is_file():
        return HTMLResponse(content=html_path.read_text(), status_code=200)
    return HTMLResponse(content="HTML file not found", status_code=404)


@app.get('/cms-b', response_class=JSONResponse)
async def cms_b(request: Request):
    with open("Data/API/cms_b.json", "r") as f:
        data = json.load(f)
    return data


@app.get('/cms-d', response_class=JSONResponse)
async def cms_d(request: Request):
    with open("Data/API/cms_d.json", "r") as f:
        data = json.load(f)
    return data


@app.get('/nucc', response_class=JSONResponse)
async def nucc(request: Request):
    with open("Data/API/nucc.json", "r") as f:
        data = json.load(f)
    return data


@app.post('/cms-b-selected', response_class=JSONResponse)
async def cms_b_selected(request: Request):
    data = await request.json()
    selected_codes = data.get("selected_codes", [])
    brand_names = data.get("brand_names", [])
    print(selected_codes)
    print(brand_names)
    return {"count": len(selected_codes)}
