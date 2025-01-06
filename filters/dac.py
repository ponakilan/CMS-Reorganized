from pyspark.sql.functions import *
from pyspark.sql import DataFrame


class DACDataProcessor:

    def __init__(self, dac: DataFrame):
        self.dac = dac

        self.dac_processed = None

    def process(self):

        dac = self.dac.select([
            "NPI",
            "Facility Name",
            "org_pac_id"
        ])

        dac_dropped = dac.dropna().dropDuplicates()

        # Convert org_pac_id to 'int'
        dac_dropped = dac_dropped.withColumn(
            "org_pac_id",
            floor(dac_dropped.org_pac_id)
        )

        grouped_df = dac_dropped.groupBy('NPI').agg(
            collect_list("Facility Name").alias("Facility_Names"),
            collect_list("org_pac_id").alias("org_pac_ids")
        )

        max_length = 4

        for i in range(max_length):
            grouped_df = grouped_df.withColumn(f"Facility_Name_{i + 1}", expr(f"Facility_Names[{i}]")).withColumn(
                f"Org_pac_id_{i + 1}", expr(f"org_pac_ids[{i}]"))

        grouped_df = grouped_df.drop("Facility_Names", "org_pac_ids")

        return grouped_df

    def merge(self, phase_1: DataFrame):

        if not self.dac_processed:
            self.dac_processed = self.process()

        grouped_df = self.dac_processed.drop("Facility_Names", "org_pac_ids")
        merged_inner = phase_1.join(
            grouped_df,
            "NPI",
            "inner"
        )

        columns = phase_1.columns[:14]
        g_cols = grouped_df.columns
        g_cols.remove('NPI')
        columns.extend(g_cols)
        columns.extend(phase_1.columns[14:])

        merged_inner = merged_inner.select(columns)

        return merged_inner
