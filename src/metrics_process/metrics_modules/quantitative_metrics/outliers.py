from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# Adiciona DoubleType
from pyspark.sql.types import DoubleType
from typing import Dict, Any, List, Union, Tuple
from config.spark import Logging
# Importa a classe DataDistribution para reutilizar cálculos
from .distribution import DataDistribution
import numpy as np # Necessário para isnan/isinf

class DataOutliers:
    def __init__(self, df: DataFrame, column: str):
        if column not in df.columns:
            Logging.error(f"Coluna '{column}' não encontrada no DataFrame.")
            self.df = None # Indica falha
            self.column = column
            return

        self.df = df
        self.column = column
        self.total_registers = self.df.count()
        # Instancia DataDistribution para acessar médias, desvios e quantis
        # DataDistribution já lida com o casting internamente
        self.dist_analyzer = DataDistribution(df, column)
        # Mantém a coluna numérica para uso local
        self.numeric_col = F.col(self.column).cast(DoubleType())


    def _get_outlier_count_and_percentage(self, lower_bound: float, upper_bound: float) -> Tuple[int, float]:
        """Helper distribuído para contar outliers dados os limites."""
        if self.df is None or self.total_registers == 0:
            return 0, 0.0

        # Trata limites NaN ou Infinitos que podem vir do IQR
        if np.isnan(lower_bound) or np.isinf(lower_bound): lower_bound = -float('inf')
        if np.isnan(upper_bound) or np.isinf(upper_bound): upper_bound = float('inf')

        try:
            # Filtra usando a coluna convertida para numérica
            outlier_count = self.df.filter(
                (self.numeric_col < lower_bound) | (self.numeric_col > upper_bound)
            ).count()
            outlier_percentage = (outlier_count / self.total_registers) * 100
            return outlier_count, outlier_percentage
        except Exception as e:
             Logging.error(f"Erro ao contar outliers para '{self.column}' com limites [{lower_bound}, {upper_bound}]: {e}")
             return 0, 0.0


    # --- Z-SCORE ---
    def z_score_outliers_percentage(self, z_threshold: float = 3.0) -> float:
        """Calcula a porcentagem de outliers baseada no Z-Score."""
        if self.df is None or self.total_registers == 0: return 0.0

        mean = self.dist_analyzer.mean()
        std_dev = self.dist_analyzer.standard_deviation()

        if std_dev == 0: return 0.0

        try:
            # Filtra usando a coluna numérica
            outlier_count = self.df.filter(
                F.abs((self.numeric_col - mean) / std_dev) > z_threshold
            ).count()
            return (outlier_count / self.total_registers) * 100
        except Exception as e:
            Logging.error(f"Erro ao calcular z_score_outliers_percentage para '{self.column}': {e}")
            return 0.0

    def z_score_outlier_count(self, z_threshold: float = 3.0) -> int:
        percentage = self.z_score_outliers_percentage(z_threshold)
        return int(round((percentage / 100.0) * self.total_registers)) if self.total_registers > 0 else 0


    # --- IQR ---
    def iqr_outlier_thresholds(self, k: float = 1.5) -> Tuple[float, float]:
        if self.df is None: return (-float('inf'), float('inf')) # Retorna infinito se falhou
        # Reutiliza o método tukey_fences que já lida com erros de quantil
        return self.dist_analyzer.tukey_fences(k)

    def iqr_outlier_percentage(self, k: float = 1.5) -> float:
        if self.df is None: return 0.0
        lower_bound, upper_bound = self.iqr_outlier_thresholds(k)
        # Se os limites forem infinitos (erro no cálculo de quantil), retorna 0%
        if lower_bound == -float('inf') and upper_bound == float('inf') and self.dist_analyzer._get_quantiles() == [0.0, 0.0, 0.0]:
             return 0.0
        _, percentage = self._get_outlier_count_and_percentage(lower_bound, upper_bound)
        return percentage

    def iqr_outlier_count(self, k: float = 1.5) -> int:
        if self.df is None: return 0
        lower_bound, upper_bound = self.iqr_outlier_thresholds(k)
        if lower_bound == -float('inf') and upper_bound == float('inf') and self.dist_analyzer._get_quantiles() == [0.0, 0.0, 0.0]:
             return 0
        count, _ = self._get_outlier_count_and_percentage(lower_bound, upper_bound)
        return count


    # --- HARD CHECK ---
    def hard_check_outlier_percentage(self, lower_limit: float, upper_limit: float) -> float:
        if self.df is None: return 0.0
        _, percentage = self._get_outlier_count_and_percentage(lower_limit, upper_limit)
        return percentage

    def hard_check_outlier_count(self, lower_limit: float, upper_limit: float) -> int:
        if self.df is None: return 0
        count, _ = self._get_outlier_count_and_percentage(lower_limit, upper_limit)
        return count
