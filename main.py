# Import local dependencies
from filters.utils import shape
from filters.openpay import OpenPayDataProcessor
from filters.nppes_nucc import NPPESNUCCDataProcessor
from filters.tax_code import TaxCodeDataProcessor
from filters.cms import CMSBDataProcessor, CMSDDataProcessor

# Import built-in dependencies
import time

# Import external dependencies
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Start timer
start = time.time()

# Create a spark session
spark = SparkSession.builder.appName("CMS-Reorganized").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the path for required files
public_dir = "../CMS-Classic/Data/Public"
private_workbook = "../CMS-Classic/Data/Private/filtering_info.xlsx"

public_files = {
    "cms_b": "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv",
    "cms_d": "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
    "openpay": "OP_DTL_GNRL_PGYR2023_P06282024_06122024.csv",
    "nppes": "npidata_pfile_20050523-20241110.csv",
    "nucc": "nucc_taxonomy_241.csv"
}

private_sheets = {
    "cms_b": 0,
    "cms_d": 1,
    "openpay": 2,
    "taxonomy": 3
}


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
cms_b_processor = CMSBDataProcessor(
    cms_b=csv_2_df("cms_b"),
    interested_drugs=excel_2_df("cms_b")
)
cms_b_full = cms_b_processor.process()

# Processing CMS-D data
cms_d_processor = CMSDDataProcessor(
    cms_d=csv_2_df("cms_d"),
    interested_drugs=excel_2_df("cms_d")
)
cms_d_full = cms_d_processor.process()

# Testing
print(f"Shape of CMS-B after processing: {shape(cms_b_full)}")
print(f"Shape of CMS-D after processing: {shape(cms_d_full)}")

"""
## Combining CMS-B and CMS-D
This block processes CMS data by merging two datasets (`cms_b_corrected` and `cms_d_full`) into `cms_combined` and grouping it by `NPI` and `Drug_Name` to aggregate total beneficiaries (`Tot_Benes`) and claims (`Tot_Clms`). 
The grouped data is pivoted on `Drug_Name` to create a wide-format table, `cms_pivot`, with sums for each drug. Missing values are filled with zeros, and null values are checked.
"""
# Merging CMS-B and CMS-D
cms_combined = cms_b_full.union(cms_d_full)
cms_grouped = cms_combined.groupBy("NPI", "Drug_Name").agg(
    sum("Tot_Benes").alias("Tot_Benes"),
    sum("Tot_Clms").alias("Tot_Clms")
)
cms_pivot = (
    cms_grouped
    .groupBy("NPI")
    .pivot("Drug_Name")
    .sum("Tot_Benes", "Tot_Clms")
)
cms_pivot = cms_pivot.fillna(value=0)

"""
## Open Payments Data
Only required columns are selected from the *Open Payments* data and the records containing **NULL** values in `Covered_Recipient_NPI` are dropped.
The data is filtered based on interested drugs and then columns are renamed for convenience. 
The data is pivoted on `Nature_Of_Payment` and `Covered_Recipient_NPI`.
"""
# Process Open Payments data
openpay_processor = OpenPayDataProcessor(
    openpay=csv_2_df("openpay"),
    interested_drugs=excel_2_df("openpay")
)
openpay_processed = openpay_processor.process()

# Testing
print(f"Shape of Open Payments after processing: {shape(openpay_processed)}")

"""
## Merging CMS and OpenPayments
The CMS and Open Payments data are combined using an outer join on NPI.
The columns Covered_Recipient_NPI and NPI are combined since they both contain NPIs and the null values are filled with 0.
"""
# Outer join based on 'NPI'
openpay_cms = openpay_processed.join(
    cms_pivot,
    openpay_processed.Covered_Recipient_NPI == cms_pivot.NPI,
    "outer"
)

# Combine both the NPI fields
openpay_cms_combined_npi = openpay_cms.withColumn(
    "Covered_Recipient_NPI",
    coalesce(
        openpay_cms['Covered_Recipient_NPI'],
        openpay_cms['NPI']
    )
)

# Drop the duplicate NPI field and fill null values with 0
openpay_cms_drop_NPI = openpay_cms_combined_npi.drop("NPI")
openpay_cms_final = openpay_cms_drop_NPI.fillna(value=0)

"""
Merging NPPES data and NUCC data
"""
nppes_nucc_processor = NPPESNUCCDataProcessor(
    nppes=csv_2_df("nppes"),
    nucc=csv_2_df("nucc")
)
nppes_nucc = nppes_nucc_processor.merge_nppes_nucc()

"""
Filtering records containing required taxonomy codes
"""
tax_code_processor = TaxCodeDataProcessor(
    tax_codes=excel_2_df("taxonomy"),
    nppes_nucc=nppes_nucc
)
tax_code_filtered = tax_code_processor.process()

"""
Merging the filtered data with CMS
"""
# Merge with CMS data and drop the duplicate NPI column
final_joined = tax_code_filtered.join(
    openpay_cms_final,
    tax_code_filtered.NPI == openpay_cms_final.Covered_Recipient_NPI,
    'inner'
)
final = final_joined.drop("Covered_Recipient_NPI")

# Renaming the columns
for col in final.columns:
    if "Tot_Benes" in col:
        final = final.withColumnRenamed(col, col.replace("Tot_Benes", "Patients"))
    if "Tot_Clms" in col:
        final = final.withColumnRenamed(col, col.replace("Tot_Clms", "Claims"))

for col in final.columns:
    if "sum(" in col:
        final = final.withColumnRenamed(col, col.replace("sum(", "").replace(")", ""))

# Generate the output file
final.toPandas().to_csv("final_out.csv")

# End timer
end = time.time()

print(f"Output of shape {shape(final)} saved to final_out.csv")
print(f"Processing completed in {end - start} seconds.")
