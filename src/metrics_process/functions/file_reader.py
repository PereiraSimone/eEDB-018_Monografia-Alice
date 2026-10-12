# metrics_process/functions/file_reader.py

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any
from config.spark import Logging
from metrics_process.model.dataset import Dataset 

class FileReader:
    """Classe responsável por ler diferentes formatos de arquivo e retornar um DataFrame Spark."""
    def __init__(self, spark: SparkSession):
        if not isinstance(spark, SparkSession):
            raise ValueError("O argumento 'spark' deve ser uma instância de SparkSession.")
        self.spark = spark
        
        self._reader_map = {
            "csv": self._read_generic,
            "parquet": self._read_generic,
            "json": self._read_generic,
            "orc": self._read_generic,
            "avro": self._read_generic,
            "text": self._read_generic,
        }

    def read(self, dataset: Dataset) -> Optional[DataFrame]:
        """Lê os dados com base no formato especificado no objeto Dataset."""
        if not dataset.source_path or not dataset.format:
            Logging.error("Erro: 'source_path' e 'format' são obrigatórios no ficheiro de configuração.")
            return None

        file_format = dataset.format.lower()
        reader_method = self._reader_map.get(file_format)

        if not reader_method:
            Logging.error(f"Erro: Formato de arquivo '{file_format}' não suportado.")
            return None

        Logging.info(f"Lendo dados de: {dataset.source_path} (Formato: {file_format}) com opções: {dataset.options}")
        
        try:
            df = reader_method(file_format, dataset.source_path, dataset.options)
            if df:
                Logging.info("Leitura concluída. DataFrame será cacheado para as validações.")
                df.cache()
            return df
        except Exception as e:
            Logging.error(f"Erro ao ler os dados de {dataset.source_path}: {e}", exc_info=True)
            return None

    def _read_generic(self, file_format: str, path: str, options: Dict[str, Any]) -> DataFrame:
        """Método de leitura genérico para formatos nativos do Spark."""
        return self.spark.read.options(**options).format(file_format).load(path)