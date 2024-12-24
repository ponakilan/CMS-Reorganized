import os
import json

import pandas as pd
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Jobs-Scheduler').getOrCreate()
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
    categories = data.get("categories",[])
    drugs=data.get("drugs",[])
    print(categories)
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

@app.get('/submitted-jobs', response_class=JSONResponse)
async def submitted_jobs(request: Request):
    data = await request.json()

    input_dir = "Data/Private/Input"
    output_dir = "Data/Private/Output"

    files_inp = {
        "username": [],
        "job_id": [],
        "in_time": []
    }
    for file in os.listdir(input_dir):
        username, time, uuid = file.split('_')
        files_inp['username'].append(username)
        files_inp['in_time'].append(time)
        files_inp['job_id'].append(uuid)

    files_out = {
        "username": [],
        "job_id": [],
        "out_time": []
    }
    for file in os.listdir(output_dir):
        username, time, uuid = file.split('_')
        files_out['username'].append(username)
        files_out['out_time'].append(time)
        files_out['job_id'].append(uuid)

    inp_df = spark.createDataFrame(pd.DataFrame(files_inp))
    out_df = spark.createDataFrame(pd.DataFrame(files_out))

    df = inp_df.join(out_df.drop("username"), "job_id", 'left')

    username = data['username']
    df = df.filter(col("username") == username)

    return {
        "jobs": json.loads(df.toPandas().to_json(orient="records"))
    }
