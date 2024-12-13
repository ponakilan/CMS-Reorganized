from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class OpenPayDataProcessor:

    def __init__(self, openpay: DataFrame, interested_drugs: DataFrame):
        self.open_payments = openpay
        self.interested_drugs = interested_drugs

        self.open_payments_filtered = None
        self.open_payments_processed = None

    def filter_interested_drugs(self, enforce=False):
        if not self.open_payments_filtered or enforce:
            open_payments_selected = self.open_payments.select([
                "Covered_Recipient_NPI",
                "Total_Amount_of_Payment_USDollars",
                "Date_of_Payment",
                "Number_of_Payments_Included_in_Total_Amount",
                "Form_of_Payment_or_Transfer_of_Value",
                "Nature_of_Payment_or_Transfer_of_Value",
                "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"
            ])

            # Drop entries with null values
            open_payments_dropped = open_payments_selected.dropna(subset="Covered_Recipient_NPI")

            # Filter based on interested drugs
            open_payments_filtered = open_payments_dropped.join(
                self.interested_drugs,
                open_payments_dropped.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 == self.interested_drugs.Drug_Name,
                "inner"
            )
            self.open_payments_filtered = open_payments_filtered.drop("Drug_Name")

        return self.open_payments_filtered

    def process(self, enforce=False):
        if not self.open_payments_processed or enforce:

            if not self.open_payments_filtered:
                self.filter_interested_drugs()

            open_payments_mapped = self.open_payments_filtered.withColumn(
                "Nature_of_Payment",
                when(col("Nature_of_Payment_or_Transfer_of_Value") == "Food and Beverage", "FOOD&BEVERAGE")
                .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Consulting Fee", "CONSULTING")
                .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Travel and Lodging", "TRAVEL")
                .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Education", "EDUCATION")
                .when(col("Nature_of_Payment_or_Transfer_of_Value").rlike("Compensation"), "SPEAKER")
                .otherwise("OTHERS_GENERAL")
            )

            # Convert 'Total_Amount_of_Payment_USDollars' to 'int'
            open_payments_casted = open_payments_mapped.withColumn(
                "Total_Amount_of_Payment_USDollars",
                open_payments_mapped.Total_Amount_of_Payment_USDollars.cast('int')
            )

            # Pivot 'Nature_of_Payment' and fill null values with 0
            open_payments_pivot_1 = (
                open_payments_casted
                .groupBy("Covered_Recipient_NPI", "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1")
                .pivot("Nature_of_Payment")
                .sum("Total_Amount_of_Payment_USDollars")
                .fillna(value=0)
            )

            # Pivot 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1'
            self.open_payments_processed = (
                open_payments_pivot_1
                .groupBy("Covered_Recipient_NPI")
                .pivot("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1")
                .sum('CONSULTING', 'EDUCATION', 'FOOD&BEVERAGE', 'OTHERS_GENERAL', 'SPEAKER', 'TRAVEL')
            )

        return self.open_payments_processed

    def merge_cms(self, cms: DataFrame, open_payments_processed: DataFrame):
        # Outer join based on 'NPI'
        openpay_cms = open_payments_processed.join(
            cms,
            open_payments_processed.Covered_Recipient_NPI == cms.NPI,
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
        openpay_cms = openpay_cms_drop_NPI.fillna(value=0)

        return openpay_cms
