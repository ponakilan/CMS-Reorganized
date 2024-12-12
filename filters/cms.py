# Import external dependencies
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class CMSBDataProcessor:

    def __init__(self, cms_b: DataFrame, interested_drugs: DataFrame):
        self.cms_b = cms_b
        self.interested_drugs = interested_drugs
        self.cms_b_filtered = None
        self.cms_b_corrected = None
        self.cms_b_grouped = None

    def filter_interested_drugs(self, enforce=False):
        if not self.cms_b_filtered or enforce:
            self.cms_b_filtered = self.cms_b.join(
                self.interested_drugs,
                'HCPCs_Cd',
                'inner'
            )
        return self.cms_b_filtered

    def process(self, enforce=False):
        if not self.cms_b_corrected or enforce:

            if not self.cms_b_filtered:
                self.filter_interested_drugs()

            cms_b_selected = self.cms_b_filtered.select([
                'Rndrng_NPI',
                'BRAND',
                'Tot_Benes',
                'Tot_Srvcs'
            ])
            cms_b_renamed = (
                cms_b_selected
                .withColumnRenamed("Rndrng_NPI", "NPI")
                .withColumnRenamed("BRAND", "Drug_Name")
                .withColumnRenamed("Tot_Srvcs", "Tot_Clms")
            )
            self.cms_b_corrected = cms_b_renamed.withColumn(
                "Tot_Clms",
                cms_b_renamed.Tot_Clms.cast('int')
            )
        return self.cms_b_corrected

    def group_data(self, enforce=False):
        if not self.cms_b_grouped or enforce:

            if not self.cms_b_corrected:
                self.process()

            self.cms_b_grouped = self.cms_b_corrected.groupby("Drug_Name").agg(
                sum("Tot_Benes").alias("Tot_Benes"),
                sum("Tot_Clms").alias("Tot_Clms")
            )
        return self.cms_b_grouped


class CMSDDataProcessor:

    def __init__(self, cms_d: DataFrame, interested_drugs: DataFrame):
        self.cms_d = cms_d
        self.interested_drugs = interested_drugs
        self.cms_d_filtered = None
        self.cms_d_corrected = None
        self.cms_d_grouped = None

    def filter_interested_drugs(self, enforce=False):
        if not self.cms_d_filtered or enforce:
            # Select the required columns
            cms_d_selected = self.cms_d.select(["Prscrbr_NPI", "Brnd_Name", "Gnrc_Name", "Tot_Clms", "Tot_Benes"])

            # Convert the 'Brnd_Name' field to lowercase
            cms_d_lower = cms_d_selected.withColumn(
                "Brnd_Name",
                lower(cms_d_selected["Brnd_Name"])
            )

            # Filter based on interested drugs
            self.cms_d_filtered = cms_d_lower.join(
                self.interested_drugs,
                "Brnd_Name",
                "inner"
            )

        return self.cms_d_filtered

    def process(self, enforce=False):
        if not self.cms_d_corrected or enforce:

            if not self.cms_d_filtered:
                self.filter_interested_drugs()

            cms_d_selected = self.cms_d_filtered.select(["Prscrbr_NPI", "BRAND", "Tot_Benes", "Tot_Clms"])

            # Rename the columns
            cms_d_renamed = (
                cms_d_selected
                .withColumnRenamed("Prscrbr_NPI", "NPI")
                .withColumnRenamed("BRAND", "Drug_Name")
            )

            # Replace null values with 0
            self.cms_d_corrected = cms_d_renamed.fillna(
                value=0,
                subset="Tot_Benes"
            )

        return self.cms_d_corrected

    def group_data(self, enforce=False):
        if not self.cms_d_grouped or enforce:

            if not self.cms_d_corrected:
                self.process()

            self.cms_d_grouped = self.cms_d_corrected.groupby("Drug_Name").agg(
                sum("Tot_Benes").alias("Tot_Benes"),
                sum("Tot_Clms").alias("Tot_Clms")
            )

        return self.cms_d_grouped
