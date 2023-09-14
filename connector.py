from pyspark import SparkConf

from pyspark.sql import SparkSession, DataFrame

from pathlib import Path
from typing import Collection, Mapping, Union

from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    IntegerType,
    ShortType,
    StringType,
)

from pyspark.sql import types as T
import pyspark.sql.functions as F


def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
        
        
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

    return df

spark = (SparkSession.builder.config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.hadoop:hadoop-aws:3.1.2",
                    "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
                    "net.snowflake:snowflake-jdbc:3.13.3",
                ]
            ),
        )
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .appName("test")
        .getOrCreate()
    )

if __name__ == "__main__":

    df = spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/")
    df2 = flatten(df)
    df3 = df2.withColumn("date_utc", psf.to_date(psf.col("date_utc"))) 
    df3.show(5)