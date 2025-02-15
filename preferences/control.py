# Import local dependencies
from filters.utils import shape
from filters.openpay import OpenPayDataProcessor
from filters.dac import DACDataProcessor
from filters.nppes_nucc import NPPESNUCCDataProcessor
from filters.tax_code import TaxCodeDataProcessor
from filters.cms import CMSBDataProcessor, CMSDDataProcessor, merge_cms
from preferences.models import Job

# Import built-in dependencies
import os
import sys
import time
import datetime

# Import external dependencies
import gdown
import pandas as pd
from pyspark.sql import SparkSession
from django.utils import timezone


def initiate_processing(
        public_dir: str,
        private_workbook: str,
        public_files: dict,
        private_sheets: dict,
        file_name: str,
        job_id: str
):
    # Start timer
    start = time.time()

    # Check for datasets and download if not available
    if not os.path.exists(f"{public_dir}/{public_files['cms_b']}"):
        print("Public datasets not found.\nDownloading the public dataset files...")
        url = 'https://drive.google.com/uc?id=12hG0YQHx8j5bF8Ot-NzkSD3Sr4OMV0Dx'
        output = 'Public.7z'
        gdown.download(url, output, quiet=False)
        print("Extracting the public dataset files...")
        os.system("7z x Public.7z -oData/Public/")

    # Create a spark session
    spark = SparkSession.builder.appName(f"CMS-{job_id}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def csv_2_df(key: str):
        return spark.read.csv(
            f"{public_dir}/{public_files[key]}",
            header=True,
            inferSchema=True
        )


    def excel_2_df(key: str):
        pd_df = pd.read_excel(
            private_workbook,
            sheet_name=private_sheets[key]
        )
        return spark.createDataFrame(pd_df)

    # Processing CMS-B data
    if private_sheets["cms_b"]:
        cms_b_processor = CMSBDataProcessor(
            cms_b=csv_2_df("cms_b"),
            interested_drugs=excel_2_df("cms_b")
        )
        cms_b_full = cms_b_processor.process()
    else:
        cms_b_full = None

    # Processing CMS-D data
    if private_sheets["cms_d"]:
        cms_d_processor = CMSDDataProcessor(
            cms_d=csv_2_df("cms_d"),
            interested_drugs=excel_2_df("cms_d")
        )
        cms_d_full = cms_d_processor.process()
    else:
        cms_d_full = None

    # Testing
    if private_sheets["cms_b"]:
        print(f"Shape of CMS-B after processing: {shape(cms_b_full)}")
    else:
        print("Skipped CMS Part-B.")
    
    if private_sheets["cms_d"]:
        print(f"Shape of CMS-D after processing: {shape(cms_d_full)}")
    else:
        print("Skipped CMS Part-D.")

    # Merge CMS-B and CMS-D
    if cms_b_full and cms_d_full:
        cms_merged = merge_cms(cms_b_full, cms_d_full)
    elif cms_b_full:
        cms_merged = cms_b_full
    elif cms_d_full:
        cms_merged = cms_d_full
    else:
        cms_merged = None

    # Process Open Payments data
    openpay_processor = OpenPayDataProcessor(
        openpay=csv_2_df("openpay"),
        interested_drugs=excel_2_df("openpay"),
        mappings=excel_2_df("openpay-map")
    )
    openpay_processed = openpay_processor.process()

    # Testing
    print(f"Shape of Open Payments after processing: {shape(openpay_processed)}")

    # Merge Openpay and CMS
    if cms_merged:
        openpay_cms = openpay_processor.merge_cms(cms_merged, openpay_processed)
    else:
        openpay_cms = openpay_processor.open_payments_processed

    # Merge NPPES and NUCC
    nppes_nucc_processor = NPPESNUCCDataProcessor(
        nppes=csv_2_df("nppes"),
        nucc=csv_2_df("nucc")
    )
    nppes_nucc = nppes_nucc_processor.merge_nppes_nucc()

    # Filter based on taxonomy codes
    try:
        tax_code_processor = TaxCodeDataProcessor(
            tax_codes=excel_2_df("taxonomy"),
            nppes_nucc=nppes_nucc
        )
    except:
        tax_code_processor = TaxCodeDataProcessor(
            tax_codes=None,
            nppes_nucc=nppes_nucc
        )
    tax_code_filtered = tax_code_processor.process()

    temp_file = f"{job_id}_{public_files['phase-1']}"
    # Merge with openpay_cms
    openpay_cms_taxcode = tax_code_processor.merge_openpay_cms(tax_code_filtered, openpay_cms)
    openpay_cms_taxcode.toPandas().to_csv(temp_file, index=False)

    # Merge Physician compare data
    dac_processor = DACDataProcessor(dac=csv_2_df("dac"))
    final = dac_processor.merge(
        phase_1=spark.read.csv(
            temp_file,
            header=True,
            inferSchema=True
        )
    )
    output = f"/static/output/{job_id}_{file_name}.csv"

    # Export the final file
    split = 22
    final = final.toPandas()
    cleaned = final[final.iloc[:, split:].sum(axis=1) > 0]
    cleaned.to_csv(output[1:], index=False)

    # End timer
    end = time.time()

    # Update the record in database
    job = Job.objects.get(job_id=job_id)
    job.out_time = timezone.make_aware(datetime.datetime.now())
    job.output_link = output
    job.save()

    print(f"Output of shape {shape(final)} saved to {output}")
    print(f"Processing completed in {end - start} seconds.")

    os.remove(temp_file)


def main(public_dir, private_workbook, output):
    public_files = {
        "cms_b": "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv",
        "cms_d": "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
        "openpay": "OP_DTL_GNRL_PGYR2023_P06282024_06122024.csv",
        "nppes": "npidata_pfile_20050523-20241208.csv",
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

    initiate_processing(
        public_dir,
        private_workbook,
        public_files,
        private_sheets,
        output
    )


if __name__ == '__main__':
    public_dir = sys.argv[1]
    private_workbook = sys.argv[2]
    output = sys.argv[3]
    main(public_dir, private_workbook, output)
