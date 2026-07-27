import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.spark import create_spark_session, Logging


def parse_args():
    """Analisa os argumentos da linha de comando para o job de rotulagem."""
    parser = argparse.ArgumentParser(description="Rotula os resultados históricos de DQ para treino de ML.")
    parser.add_argument("--historical-path", required=True, help="Caminho S3 para os resultados históricos brutos (Parquet).")
    parser.add_argument("--labeled-path", required=True, help="Caminho S3 para salvar os resultados rotulados (Parquet).")
    return parser.parse_args()

def trainner_run():
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("HAL9000-Labeling")
        
        Logging.info(f"Carregando resultados históricos de: {args.historical_path}")
        historical_df = spark.read.parquet(args.historical_path)

        # Lógica de Rotulagem (Exemplo - AJUSTE CONFORME A SUA LÓGICA DE NEGÓCIO)
        # Aqui, você define o que constitui um "problema real" vs. um "falso positivo".
        Logging.info("Aplicando lógica de rotulagem...")
        labeled_df = historical_df.withColumn(
            "true_label",
            F.when(
                (F.col("status") == "FAIL") & 
                (F.col("metric") == "DataConsistency.uniqueness_consistency_percentage") &
                (F.col("column") == "CNPJ"), 
                1.0 # Exemplo: Falha de unicidade no CNPJ é um PROBLEMA REAL
            ).when(
                (F.col("status") == "FAIL") &
                (F.col("metric") == "DataOutliers.iqr_outlier_percentage") &
                (F.col("column") == "CEP"),
                0.0 # Exemplo: Outliers no CEP são FALSOS POSITIVOS
            ).otherwise(None) 
        ).filter(F.col("true_label").isNotNull()) # Mantém apenas os registos que conseguimos rotular

        Logging.info(f"Salvando resultados rotulados em: {args.labeled_path}")
        
        # CORRIGIDO: Usa o modo "append" para construir o histórico ao longo do tempo.
        labeled_df.write.mode("append").partitionBy("execution_date").parquet(args.labeled_path)
        
        Logging.info("Rotulagem concluída com sucesso.")

    except Exception as e:
        Logging.error(f"Erro durante a rotulagem: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()