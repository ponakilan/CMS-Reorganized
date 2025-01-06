from pyspark.sql import DataFrame


class TaxCodeDataProcessor:

    def __init__(self, tax_codes: DataFrame, nppes_nucc: DataFrame):
        self.tax_codes = tax_codes
        self.nppes_nucc = nppes_nucc

        self.nppes_nucc_filtered = None

    def get_required_codes(self):
        if self.tax_codes:
            required_codes = list(
                map(
                    lambda obj: obj.Code,
                    self.tax_codes.select('Code').collect()
                )
            )
            return required_codes
        return []

    def process(self, enforce=False):
        if not self.nppes_nucc_filtered or enforce:
            # Fetch the required codes
            required_codes = self.get_required_codes()

            # Only filter if taxonomy codes are given
            if len(required_codes) > 0:
                tax_code_cond = (
                        (self.nppes_nucc["Healthcare Provider Taxonomy Code_1"].isin(required_codes))
                        | (self.nppes_nucc["Healthcare Provider Taxonomy Code_2"].isin(required_codes))
                        | (self.nppes_nucc["Healthcare Provider Taxonomy Code_3"].isin(required_codes))
                )
                nppes_nucc_filtered = self.nppes_nucc.filter(tax_code_cond)
            else:
                nppes_nucc_filtered = self.nppes_nucc

            # Drop unwanted columns
            self.nppes_nucc_filtered = nppes_nucc_filtered.drop(*[
                'Code',
                'grouping',
                'display_name',
                'Healthcare Provider Taxonomy Code_1',
                'Healthcare Provider Taxonomy Code_2',
                'Healthcare Provider Taxonomy Code_3'
            ])

        return self.nppes_nucc_filtered

    def merge_openpay_cms(self, tax_code_filtered, openpay_cms):
        # Merge with CMS data and drop the duplicate NPI column
        final_joined = tax_code_filtered.join(
            openpay_cms,
            tax_code_filtered.NPI == openpay_cms.Covered_Recipient_NPI,
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

        return final
