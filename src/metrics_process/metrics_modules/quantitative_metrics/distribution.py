from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# Adiciona a importação de tipos para o casting
from pyspark.sql.types import LongType, DoubleType
from typing import List, Tuple, Dict, Union, Any
# Importa o logger para mensagens de erro
from config.spark import Logging
import numpy as np # Necessário para isnan/isinf

class DataDistribution:
    def __init__(self, df: DataFrame, column: str):
        if column not in df.columns:
            # Usa o logger importado
            Logging.error(f"Coluna '{column}' não encontrada no DataFrame.")
            # Define df como None para indicar falha na inicialização
            self.df = None
            self.column = column
            return

        self.df = df
        self.column = column
        self.total_registers = self.df.count()
        # Tenta converter a coluna para DoubleType para cálculos estatísticos
        # Se falhar (ex: coluna de texto puro), a maioria dos métodos retornará 0 ou None.
        self.numeric_col = F.col(self.column).cast(DoubleType())

    def _get_single_stat(self, func_name: str) -> float:
        """Helper para executar funções estatísticas simples (mean, min, max, stddev)."""
        if self.df is None or self.total_registers == 0:
            return 0.0

        try:
            # ==============================================================================
            # CORREÇÃO 2: Usa getattr para obter a função do módulo F dinamicamente
            # e aplica à coluna convertida para numérica.
            # ==============================================================================
            spark_func = getattr(F, func_name)
            # Aplica a função à coluna já convertida para DoubleType
            result = self.df.select(spark_func(self.numeric_col)).collect()[0][0]

            # Trata casos onde o resultado pode ser None (coluna vazia ou só nulos)
            # ou NaN/Infinito
            if result is None or np.isnan(result) or np.isinf(result):
                return 0.0
            return float(result)
        except Exception as e:
            Logging.error(f"Erro ao calcular '{func_name}' para a coluna '{self.column}': {e}")
            return 0.0 # Retorna 0 em caso de erro

    def _get_quantiles(self, probabilities: List[float] = [0.25, 0.5, 0.75]) -> List[float]:
        """Helper para obter quantis."""
        if self.df is None or self.total_registers == 0:
            return [0.0] * len(probabilities)

        try:
            # ==============================================================================
            # CORREÇÃO 1: Garante que approxQuantile seja chamado na coluna numérica.
            # Spark lançará um erro se a coluna original (antes do cast) não puder ser
            # interpretada numericamente. Lidamos com isso no try/except.
            # ==============================================================================
            # Precisamos do nome da coluna original aqui
            quantiles_result = self.df.stat.approxQuantile(self.column, probabilities, 0.01)
             # Trata Nones que podem ocorrer se a coluna for majoritariamente nula
            return [float(q) if q is not None else 0.0 for q in quantiles_result]
        except Exception as e:
            # Captura o erro específico do Spark se a coluna não for numérica
            if "Quantile calculation" in str(e) and "StringType is not supported" in str(e):
                 Logging.warning(f"Cálculo de quantil não suportado para a coluna '{self.column}' (tipo String). Retornando zeros.")
            else:
                 Logging.error(f"Erro ao calcular quantis para a coluna '{self.column}': {e}")
            return [0.0] * len(probabilities) # Retorna zeros em caso de erro


    # --- Métodos de Tendência Central ---
    def mean(self) -> float: return self._get_single_stat("avg")
    def median(self) -> float: return self._get_quantiles(probabilities=[0.5])[0]
    def variance(self) -> float: return self._get_single_stat("variance")
    def standard_deviation(self) -> float: return self._get_single_stat("stddev")
    def min_value(self) -> float: return self._get_single_stat("min")
    def max_value(self) -> float: return self._get_single_stat("max")
    def range_value(self) -> float:
        min_v = self.min_value()
        max_v = self.max_value()
        return max_v - min_v if min_v is not None and max_v is not None else 0.0

    def quartiles(self) -> Tuple[float, float, float]:
        quantiles = self._get_quantiles(probabilities=[0.25, 0.5, 0.75])
        return quantiles[0], quantiles[1], quantiles[2]

    def interquartile_range(self) -> float:
        Q1, _, Q3 = self.quartiles()
        return Q3 - Q1 if Q1 is not None and Q3 is not None else 0.0

    # --- Métodos de Forma ---
    def skewness(self) -> float:
        if self.df is None or self.total_registers == 0: return 0.0
        try:
            result = self.df.select(F.skewness(self.numeric_col)).collect()[0][0]
            return float(result) if result is not None and not np.isnan(result) and not np.isinf(result) else 0.0
        except Exception as e:
             Logging.error(f"Erro ao calcular skewness para a coluna '{self.column}': {e}")
             return 0.0

    def kurtosis(self) -> float:
        if self.df is None or self.total_registers == 0: return 0.0
        try:
            result = self.df.select(F.kurtosis(self.numeric_col)).collect()[0][0]
            return float(result) if result is not None and not np.isnan(result) and not np.isinf(result) else 0.0
        except Exception as e:
            Logging.error(f"Erro ao calcular kurtosis para a coluna '{self.column}': {e}")
            return 0.0

    # --- Métodos Categóricos ---
    def frequency_table(self) -> Dict[str, int]:
        if self.df is None: return {}
        try:
            df_freq = self.df.groupBy(self.column).count().orderBy(F.desc("count")).limit(1000)
            return {str(row[self.column]): row["count"] for row in df_freq.collect()}
        except Exception as e:
            Logging.error(f"Erro ao calcular frequency_table para a coluna '{self.column}': {e}")
            return {}

    def mode(self) -> List[Any]:
        freq_table = self.frequency_table()
        if not freq_table: return []
        max_count = max(freq_table.values())
        return [k for k, v in freq_table.items() if v == max_count]

    def percentile(self, p: float) -> float:
        if not 0 <= p <= 100:
            Logging.error("O percentil deve estar entre 0 e 100.")
            return 0.0
        return self._get_quantiles(probabilities=[p / 100.0])[0]

    # --- Z-Scores ---
    def z_scores(self) -> DataFrame:
        if self.df is None: return None # Retorna None se a inicialização falhou
        mean_val = self.mean()
        std_dev_val = self.standard_deviation()
        if std_dev_val == 0:
            return self.df.withColumn(f"{self.column}_z_score", F.lit(0.0))
        # Aplica à coluna convertida
        return self.df.withColumn(
            f"{self.column}_z_score",
            (self.numeric_col - mean_val) / std_dev_val
        )

    # --- Outliers (Referência) ---
    def tukey_fences(self, k: float = 1.5) -> Tuple[float, float]:
        Q1, _, Q3 = self.quartiles()
        IQR = self.interquartile_range()
        lower_fence = Q1 - k * IQR
        upper_fence = Q3 + k * IQR
        return lower_fence, upper_fence
