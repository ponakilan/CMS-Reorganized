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


@app.get('/openpay-cat', response_class=JSONResponse)
async def render_info(request: Request):
    with open("Data/API/openpay.json", "r") as f:
        data = json.load(f)
    return data

@app.get("/openpay-cat-page", response_class=HTMLResponse)
async def serve_html():
    html_path = Path("templates/op.html")  # Adjust the path if your file is named differently or in another directory
    if html_path.is_file():
        return HTMLResponse(content=html_path.read_text(), status_code=200)
    return HTMLResponse(content="HTML file not found", status_code=404)

@app.post('/openpay-data', response_class=JSONResponse)
async def openpay_data(request: Request):
    data = await request.json()
    selected_oname = data.get("selected_oname",[])
    selected_rename = data.get("selected_rename",[])
    drugs=data.get("drugs",[])
    print(selected_oname)
    print(selected_rename)
    print(drugs)


@app.post('/cms-all-data', response_class=JSONResponse)
async def cms_all_data(request: Request):
    data = await request.json()
    selected_codes = data.get("selected_codes", [])
    brand_names_codes = data.get("brand_names_codes", [])
    selected_brnds = data.get("selected_brnds", [])
    selected_gnrcs = data.get("selected_gnrcs", [])
    brand_names_drugs = data.get("brand_names_drugs", [])
    selected_scodes=data.get("selected_scodes",[])
    
    print(selected_codes)
    print(brand_names_codes)
    print(selected_brnds)
    print(selected_gnrcs)
    print(brand_names_drugs)
    print(selected_scodes)

    return {"count": len(selected_codes)}

""" 
@app.post('/cms-all-data', response_class=JSONResponse)
async def cms_all_data(request: Request):
    global_data = {
        "selected_codes": [],
        "brand_names_codes": [],
        "selected_brnds": [],
        "selected_gnrcs": [],
        "brand_names_drugs": [],
        "selected_scodes": [],
        "selected_original": [],
        "selected_renamed": [],
    }
    
    new_data = await request.json()

    for key in global_data:
        if key in new_data:
            global_data[key].extend(new_data[key])

    print(global_data)  # Log combined data for debugging
    return {"status": "Data received successfully", "combined_count": len(global_data)}

"""