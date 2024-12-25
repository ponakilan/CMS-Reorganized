import os
import json
from datetime import datetime
from uuid import uuid4
import pickle

from preferences.control import initiate_processing

import pandas as pd
from fastapi import FastAPI, Request
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

    username = "ponakilan"
    categories = data.get("categories",[])
    drugs=data.get("drugs",[])

    original_category = []
    renamed_category = []
    for data in categories:
        original_category.append(data['Original'])
        renamed_category.append(data['Renamed'])

    with open(f"Data/API/{username}.pkl", "rb") as f:
        data_dict = pickle.load(f)

    # Writing CMS-B Data
    cms_b_df = pd.DataFrame({
        "HCPCS_Cd": data_dict['cms_b_codes'],
        "BRAND": data_dict['cms_b_brands']
    })
    cms_d_df = pd.DataFrame({
        "Brnd_Name": data_dict['cms_d_brnds'],
        "Gnrc_Name": data_dict['cms_d_gnrcs'],
        "BRAND": data_dict['cms_d_brands']
    })
    openpay_drug_df = pd.DataFrame({
        "Drug_Name": drugs
    })
    tax_code_df = pd.DataFrame({
        "Code": data_dict['taxonomy_codes']
    })
    openpay_map_df = pd.DataFrame({
        "Original": original_category,
        "Renamed": renamed_category
    })
    job_id = uuid4()
    filename = f"Data/Private/Input/{username}_{datetime.now()}_{job_id}.xlsx"
    with pd.ExcelWriter(filename) as writer:
        cms_b_df.to_excel(writer, sheet_name="CMS_B_Unique_HCPCS", index=False)
        cms_d_df.to_excel(writer, sheet_name="CMS_D_Gnrc_Names", index=False)
        openpay_drug_df.to_excel(writer, sheet_name="Openpayments_Drug_Mappings", index=False)
        tax_code_df.to_excel(writer, sheet_name="Taxonomy_Codes", index=False)
        openpay_map_df.to_excel(writer, sheet_name="Opanpay_Mappings", index=False)

    # Trigger the processing
    public_files = {
        "cms_b": "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv",
        "cms_d": "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
        "openpay": "OP_DTL_GNRL_PGYR2023_P06282024_06122024.csv",
        "nppes": "npidata_pfile_20050523-20241110.csv",
        "nucc": "nucc_taxonomy_241.csv",
        "dac": "DAC_NationalDownloadableFile.csv",
        "phase-1": "phase_1.csv"
    }
    private_sheets = {
        "cms_b": 0,
        "cms_d": 1,
        "openpay": 2,
        "taxonomy": 3,
        "openpay-map": 4
    }
    initiate_processing(
        public_dir="Data/Public",
        private_workbook=filename,
        public_files=public_files,
        private_sheets=private_sheets,
        username=username,
        job_id=str(job_id)
    )

@app.post('/cms-all-data', response_class=JSONResponse)
async def cms_all_data(request: Request):
    data = await request.json()

    username = "ponakilan"
    selected_codes = data.get("selected_codes", [])
    brand_names_codes = data.get("brand_names_codes", [])
    selected_brnds = data.get("selected_brnds", [])
    selected_gnrcs = data.get("selected_gnrcs", [])
    brand_names_drugs = data.get("brand_names_drugs", [])
    selected_scodes=data.get("selected_scodes",[])
    
    user_data = {
        "cms_b_codes": selected_codes,
        "cms_b_brands": brand_names_codes,
        "cms_d_brnds": selected_brnds,
        "cms_d_gnrcs": selected_gnrcs,
        "cms_d_brands": brand_names_drugs,
        "taxonomy_codes": selected_scodes
    }

    with open(f"Data/API/{username}.pkl", "wb") as f:
        pickle.dump(user_data, f)

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
        files_inp['job_id'].append(uuid[:-5])

    files_out = {
        "username": [],
        "job_id": [],
        "out_time": []
    }
    for file in os.listdir(output_dir):
        username, time, uuid = file.split('_')
        files_out['username'].append(username)
        files_out['out_time'].append(time)
        files_out['job_id'].append(uuid[:-4])

    inp_df = spark.createDataFrame(pd.DataFrame(files_inp))
    out_df = spark.createDataFrame(pd.DataFrame(files_out))

    df = inp_df.join(out_df.drop("username"), "job_id", 'left')

    username = data['username']
    df = df.filter(col("username") == username)

    return {
        "jobs": json.loads(df.toPandas().to_json(orient="records"))
    }
