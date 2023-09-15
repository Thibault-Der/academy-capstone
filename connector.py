from pyspark import SparkConf

import json

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

import boto3
from botocore.exceptions import ClientError

def get_secret():

    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        #aws_access_key_id="AKIAU5YMQWRQ4TGY2LGK",
        #aws_secret_access_key="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"
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.

    return secret

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
    
    secret = json.loads(get_secret())

    sfOptions = {
    "sfURL" : secret["URL"],
    "sfAccount" : "yw41113",
    "sfUser" : secret["USER_NAME"],
    "sfPassword" : secret["PASSWORD"],
    "sfDatabase" : secret["DATABASE"],
    "sfSchema" : "THIBAULT",
    "sfRole" : secret["ROLE"]
    }

    df3.write.format("snowflake").options(**sfOptions).option("dbtable", "THIBAULTTABLE").mode('append').options(header=True).save()
