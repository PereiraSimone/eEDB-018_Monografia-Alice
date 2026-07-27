# config/spark.py

import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
Logging = logging.getLogger(__name__)

def create_spark_session(app_name: str = "HAL9000-DQ-Engine") -> SparkSession:
    
    s3_endpoint = "http://minio:9000"
    
    # Esta configuração é para rodar DENTRO do cluster.
    # O --master é passado pelo comando spark-submit, não aqui.
    spark_builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Logging.info(f"SparkSession '{app_name}' iniciada com suporte para S3 em '{s3_endpoint}'.")
    return spark