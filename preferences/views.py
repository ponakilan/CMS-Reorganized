import json
import pickle
import threading
from uuid import uuid4
from datetime import datetime

from preferences.control import initiate_processing
from preferences.models import Job

import pandas as pd
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout


@login_required
def index(request):
    return render(request, "index.html")


def cms_b(request):
    with open("Data/API/cms_b.json", "r") as f:
        data = json.load(f)
    return JsonResponse(data)


def cms_d(request):
    with open("Data/API/cms_d.json", "r") as f:
        data = json.load(f)
    return JsonResponse(data)


def nucc(request):
    with open("Data/API/nucc.json", "r") as f:
        data = json.load(f)
    return JsonResponse(data)


def openpay_cat(request):
    with open("Data/API/openpay.json", "r") as f:
        data = json.load(f)
    return JsonResponse(data)


def openpay_cat_html(request):
    return render(request, "op.html")


def openpay_data(request):
    username = request.user.username
    data = json.loads(request.body)
    categories = data.get("categories", [])
    drugs = data.get("drugs", [])
    file_name = data.get("file_name", "")

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

    print(data_dict['cms_b_codes'])
    print(data_dict['cms_d_brnds'])

    cms_b = len(data_dict['cms_b_codes']) > 0
    cms_d = len(data_dict['cms_d_brnds']) > 0

    job_id = uuid4()
    in_time = datetime.now()
    filename = f"static/input/{job_id}_{file_name}.xlsx"

    with pd.ExcelWriter(filename) as writer:
        if cms_b:
            cms_b_df.to_excel(writer, sheet_name="CMS_B_Unique_HCPCS", index=False)
        if cms_d:
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
        "cms_b": "CMS_B_Unique_HCPCS" if cms_b else None,
        "cms_d": "CMS_D_Gnrc_Names" if cms_d else None,
        "openpay": "Openpayments_Drug_Mappings",
        "taxonomy": "Taxonomy_Codes",
        "openpay-map": "Opanpay_Mappings"
    }

    # Insert a record in the database
    job = Job(
        job_id = job_id,
        username = username,
        in_time = timezone.make_aware(in_time),
        out_time = None,
        input_link = f"/static/input/{job_id}_{file_name}.xlsx"
    )
    job.save()

    job_thread = threading.Thread(
        target=initiate_processing,
        name=f"job_{job_id}",
        args=[
            "Data/Public",
            filename,
            public_files,
            private_sheets,
            file_name,
            str(job_id)
        ]
    )
    job_thread.start()

    return HttpResponse("Job Started!")


def cms_data(request):
    username = request.user.username
    data = json.loads(request.body)
    selected_codes = data.get("selected_codes", [])
    brand_names_codes = data.get("brand_names_codes", [])
    brand_names_codes = [val.upper() for val in brand_names_codes]
    selected_brnds = data.get("selected_brnds", [])
    selected_brnds = [val.lower() for val in selected_brnds]
    selected_gnrcs = data.get("selected_gnrcs", [])
    selected_gnrcs = [val.lower() for val in selected_gnrcs]
    brand_names_drugs = data.get("brand_names_drugs", [])
    brand_names_drugs = [val.upper() for val in brand_names_drugs]
    selected_scodes = data.get("selected_scodes", [])
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

    return JsonResponse({"count": len(selected_codes)})


@login_required
def view_jobs(request):
    username = request.user.username
    jobs = Job.objects.filter(username=username).order_by('-in_time')
    return render(request, "submitted.html", {"jobs": jobs})


def test(request):
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
        "cms_b": "CMS_B_Unique_HCPCS",
        "cms_d": "CMS_D_Gnrc_Names",
        "openpay": "Openpayments_Drug_Mappings",
        "taxonomy": "Taxonomy_Codes",
        "openpay-map": "Opanpay_Mappings"
    }

    job_id = uuid4()
    in_time = datetime.now()

    # Insert a record in the database
    job = Job(
        job_id = job_id,
        username = "akilan",
        in_time = timezone.make_aware(in_time),
        out_time = None,
        input_link = f"/static/input/test.xlsx"
    )
    job.save()

    job_thread = threading.Thread(
        target=initiate_processing,
        name=f"job_{job_id}",
        args=[
            "Data/Public",
            "static/input/test.xlsx",
            public_files,
            private_sheets,
            "test",
            str(job_id)
        ]
    )
    job_thread.start()
    return HttpResponse("Starting test...")


def logout_view(request):
    logout(request)
    return redirect("/")
