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


def update_cms_d(save_location):
    cms_d = spark.read.csv(f"{public_dir}/{public_files['cms_d']}", header=True, inferSchema=True)
    cms_d_selected = cms_d.select(['Brnd_Name', 'Gnrc_Name']).distinct()
    unique_drugs = cms_d_selected.toJSON().map(lambda j: json.loads(j)).collect()
    unique_drugs = {"drugs": unique_drugs}

    with open(save_location, "w", encoding='utf-8') as f:
        json.dump(unique_drugs, f, ensure_ascii=False, indent=4)


def update_nucc(save_location):
    nucc = spark.read.csv(f"{public_dir}/{public_files['nucc']}", header=True, inferSchema=True)
    nucc_selected = nucc.select(['Code', 'Specialization'])
    nucc_selected = nucc_selected.dropna()
    unique_codes = nucc_selected.toJSON().map(lambda j: json.loads(j)).collect()
    unique_codes = {"codes": unique_codes}

    with open(save_location, "w", encoding='utf-8') as f:
        json.dump(unique_codes, f, ensure_ascii=False, indent=4)


def update_openpay(save_location):
    openpay = spark.read.csv(f"{public_dir}/{public_files['openpay']}", header=True, inferSchema=True)
    rename_mapping = {
        "Education": "EDUCATION",
        "Travel and Lodging": "TRAVEL",
        "Food and Beverage": "FOOD&BEVERAGE",
        "Consulting Fee": "CONSULTING",
        "Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program": "SPEAKER",
        "Compensation for serving as faculty or as a speaker for a medical education program": "SPEAKER"
    }
    openpay_selected = openpay.select("Nature_of_Payment_or_Transfer_of_Value").distinct().dropna()
    categories = (
        openpay_selected.rdd
        .map(lambda row: {"Original": row["Nature_of_Payment_or_Transfer_of_Value"], 
                          "Renamed": rename_mapping.get(row["Nature_of_Payment_or_Transfer_of_Value"], "")})
        .collect()
    )
    final_json = {"categories": categories}
    with open(save_location, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    update_cms_b("Data/API/cms_b.json")
    update_cms_d("Data/API/cms_d.json")
    update_nucc("Data/API/nucc.json")
    update_openpay("Data/API/openpay.json")
