import pyspark
from pyspark.sql.functions import *


def shape(dataframe: pyspark.sql.DataFrame):
    """
    Returns the shape of the dataframe as a tuple.
    """
    return dataframe.count(), len(dataframe.columns)


def is_null(dataframe: pyspark.sql.DataFrame):
    """
    Checks for null values in every field of the dataframe and prints the count.
    """
    return dataframe.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns]
    )
