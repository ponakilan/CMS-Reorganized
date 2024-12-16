import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CMS-Update-Static").getOrCreate()

public_dir = "Data/Public"
public_files = {
    "cms_b": "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv",
    "cms_d": "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
    "openpay": "OP_DTL_GNRL_PGYR2023_P06282024_06122024.csv",
    "nppes": "npidata_pfile_20050523-20241110.csv",
    "nucc": "nucc_taxonomy_241.csv",
    "dac": "DAC_NationalDownloadableFile.csv",
    "phase-1": "phase_1.csv"
}

def update_cms_b(save_location):
    cms_b = spark.read.csv(f"{public_dir}/{public_files['cms_b']}", header=True, inferSchema=True)
    cms_b_selected = cms_b.select(['HCPCS_Cd', 'HCPCS_Desc']).distinct()
    unique_drugs = cms_b_selected.toJSON().map(lambda j: json.loads(j)).collect()
    unique_drugs = {"drugs": unique_drugs}

    with open(save_location, "w", encoding='utf-8') as f:
        json.dump(unique_drugs, f, ensure_ascii=False, indent=4)
    return unique_drugs


def update_cms_d(save_location):
    cms_d = spark.read.csv(f"{public_dir}/{public_files['cms_d']}", header=True, inferSchema=True)
    cms_d_selected = cms_d.select(['Brnd_Name', 'Gnrc_Name']).distinct()
    unique_drugs = cms_d_selected.toJSON().map(lambda j: json.loads(j)).collect()
    unique_drugs = {"drugs": unique_drugs}

    with open(save_location, "w", encoding='utf-8') as f:
        json.dump(unique_drugs, f, ensure_ascii=False, indent=4)
    return unique_drugs


if __name__ == "__main__":
    update_cms_b("Data/API/cms_b.json")
    update_cms_d("Data/API/cms_d.json")
