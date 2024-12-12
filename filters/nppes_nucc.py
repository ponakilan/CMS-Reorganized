# Import external dependencies
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class NPPESNUCCDataProcessor:

    def __init__(self, nppes: DataFrame, nucc: DataFrame):
        self.nppes = nppes
        self.nucc = nucc

        self.nppes_selected = None
        self.nppes_nucc = None

    def select_fields(self, enforce=False):
        if not self.nppes_selected or enforce:
            # Select only the required columns and rename the 'Last Name' column
            nppes_selected = self.nppes.select(
                'NPI',
                'Provider Last Name (Legal Name)',
                'Provider First Name',
                'Provider Middle Name',
                'Provider First Line Business Mailing Address',
                'Provider Business Mailing Address City Name',
                'Provider Business Mailing Address State Name',
                'Provider Business Mailing Address Postal Code',
                'Healthcare Provider Taxonomy Code_1',
                'Healthcare Provider Taxonomy Code_2',
                'Healthcare Provider Taxonomy Code_3'
            )
            self.nppes_selected = nppes_selected.withColumnRenamed(
                'Provider Last Name (Legal Name)',
                'Provider Last Name'
            )
        return self.nppes_selected

    def merge_nppes_nucc(self, enforce=False):
        if not self.nppes_nucc or enforce:

            # Select the required columns if not selected
            if not self.nppes_selected:
                self.select_fields()

            # Merging NUCC and NPPES data
            nppes_nucc = self.nppes_selected.join(
                self.nucc,
                self.nppes_selected["Healthcare Provider Taxonomy Code_1"] == self.nucc.Code,
                'left'
            )
            nppes_nucc = nppes_nucc.drop('Code', 'grouping', 'display_name')
            nppes_nucc = nppes_nucc.withColumnRenamed("classification", "Primary_Classification")
            nppes_nucc = nppes_nucc.withColumnRenamed("specialization", "Primary_Specialization")

            nppes_nucc = nppes_nucc.join(
                self.nucc,
                nppes_nucc["Healthcare Provider Taxonomy Code_2"] == self.nucc.Code,
                'left'
            )
            nppes_nucc = nppes_nucc.drop('Code', 'grouping', 'display_name')
            nppes_nucc = nppes_nucc.withColumnRenamed("classification", "Secondary_Classification")
            nppes_nucc = nppes_nucc.withColumnRenamed("specialization", "Secondary_Specialization")

            nppes_nucc = nppes_nucc.join(
                self.nucc,
                nppes_nucc["Healthcare Provider Taxonomy Code_3"] == self.nucc.Code,
                'left'
            )
            nppes_nucc = nppes_nucc.drop('Code', 'grouping', 'display_name')
            nppes_nucc = nppes_nucc.withColumnRenamed("classification", "Tertiary_Classification")
            self.nppes_nucc = nppes_nucc.withColumnRenamed("specialization", "Tertiary_Specialization")

        return self.nppes_nucc
