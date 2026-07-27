# metrics_process/functions/report_writer.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime
from typing import List, Dict, Any
from config.spark import Logging
from metrics_process.metrics_modules.monitoring.reports import Reports

class ReportWriter:
    """
    Classe responsável por escrever os resultados do objeto Reports em diferentes formatos.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_as_parquet(self, reports_obj: Reports, output_path: str):
        """
        Converte os relatórios de um objeto Reports num DataFrame e salva como Parquet.
        """
        reports_list = reports_obj.get_reports()
        if not reports_list:
            Logging.warning("Nenhum relatório para salvar em Parquet.")
            return

        schema = StructType([
            StructField("source", StringType(), True),
            StructField("column", StringType(), True),
            StructField("metric", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("threshold_fail", StringType(), True),
            StructField("threshold_success", StringType(), True),
            StructField("metric_execution_time", DoubleType(), True),
            StructField("execution_timestamp", TimestampType(), True),
        ])

        report_data = []
        execution_time = datetime.now()

        for report_entry in reports_list:
            data = report_entry.get("content", {})
            report_data.append((
                data.get("source"),
                data.get("column", "N/A"),
                data.get("metric"),
                data.get("value"),
                data.get("status"),
                str(data.get("threshold_fail")),
                str(data.get("threshold_success")),
                data.get("metric_execution_time"),
                execution_time
            ))

        if not report_data:
            Logging.warning("Nenhum dado de relatório válido para salvar em Parquet.")
            return

        try:
            results_df = self.spark.createDataFrame(report_data, schema)
            results_df = results_df.withColumn("execution_date", F.to_date("execution_timestamp"))

            Logging.info(f"Salvando resultados de DQ em Parquet em: {output_path}")
            
            results_df.write.mode("append").partitionBy("execution_date").parquet(output_path)
            
            Logging.info("Resultados salvos com sucesso.")
        except Exception as e:
            Logging.error(f"Erro ao salvar resultados em Parquet: {e}", exc_info=True)