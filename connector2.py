from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, DateType
import boto3
from botocore.exceptions import ClientError
import json

def get_secret():
    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
        ])
        
        
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

    return df

if __name__ == "__main__":
    spark = SparkSession.builder.config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.1.2",
            "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
            "net.snowflake:snowflake-jdbc:3.13.3",
        ]),
    ).config(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    ).appName("test").getOrCreate()

    df = spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/")
    df2 = flatten(df)
    df3 = df2.withColumn("date_utc", F.to_date(F.col("date_utc"))) 

    secret = get_secret()
    
    sfOptions = {
        "sfURL": secret["URL"],
        "sfAccount": "yw41113",
        "sfUser": secret["USER_NAME"],
        "sfPassword": secret["PASSWORD"],
        "sfDatabase": secret["DATABASE"],
        "sfSchema": "THIBAULT",
        "sfRole": secret["ROLE"]
    }

    df3.write.format("snowflake").options(**sfOptions).option("dbtable", "THIBAULTTABLE").mode('overwrite').options(header=True).save()

    print("profit")
