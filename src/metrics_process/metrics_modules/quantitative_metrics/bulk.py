from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional, Union, Dict, Any, List
from config.spark import Logging


class Bulk:
    """
    HAL-9000: Métricas de Volumetria (Bulk).
    Esta classe lida com contagens de registros, unicidade e informações a nível de esquema.
    """

    def __init__(self, dataset: DataFrame):
        """
        Inicializa a classe com o DataFrame de trabalho.
        
        Args:
            dataset: O DataFrame PySpark a ser validado.
        """
        if dataset is None:
            return Logging.error("O DataFrame 'dataset' não pode ser None.")

        self.dataset = dataset
        # Pré-calcula a contagem total de registros para eficiência, pois count() é uma AÇÃO cara no Spark.
        self.total_registers = self.dataset.count()

    def _check_column_exists(self, column: str):
        """Helper para verificar se a coluna alvo existe."""
        if column not in self.dataset.columns:
            return Logging.error(f"Coluna '{column}' não encontrada no DataFrame.")

    # --- MÉTODOS DE CONTAGEM E UNICIDADE ---
    def count_registers(self) -> int:
        """
        Retorna a contagem total de linhas no DataFrame (equivalente ao COUNT(*)).
        """
        return self.total_registers

    def count_distinct_registers(self, column: str) -> int:
        """
        Retorna a contagem de valores distintos em uma coluna específica.
        """
        self._check_column_exists(column)
        return self.dataset.select(column).distinct().count()

    def count_duplicated_registers(self, columns: Union[str, List[str]]) -> int:
        """
        Retorna a contagem de REGISTROS que são duplicados com base na(s) coluna(s) (chave) fornecida(s).
        """
        if isinstance(columns, str):
            columns = [columns]
        
        for col in columns:
            self._check_column_exists(col)

        # Agrupa pelas colunas e filtra onde a contagem é > 1 (duplicatas). Em seguida, conta o número de registros (linhas) nessas duplicatas.
        df_duplicates = self.dataset.groupBy(columns).count().filter(F.col("count") > 1)
        num_duplicate_groups = df_duplicates.count()
        return num_duplicate_groups if num_duplicate_groups > 0 else 0


    def count_columns(self) -> int:
        """Retorna o número de colunas (campos) no DataFrame."""
        return len(self.dataset.columns)
        
    # --- MÉTODOS DE TIPO DE DADO E PREENCHIMENTO ---
    def data_types_frequency(self) -> Dict[str, str]:
        """
        Retorna um dicionário com o nome da coluna e seu tipo de dado (Schema). Ex: {'id_pedido': 'long', 'valor': 'double'}
        """
        return {name: dtype for name, dtype in self.dataset.dtypes}

    def percent_data_types(self) -> Dict[str, float]:
        """
        Retorna a frequência percentual dos tipos de dados presentes no esquema.
        Útil para monitorar a composição do esquema (Ex: {'string': 50.0, 'integer': 50.0}).
        """
        type_counts = {}
        for _, dtype in self.dataset.dtypes:
            type_counts[dtype] = type_counts.get(dtype, 0) + 1
        
        total_cols = self.count_columns()
        return {dtype: (count / total_cols) * 100 for dtype, count in type_counts.items()}
        
    # --- MÉTODOS COMPLEMENTARES ---

    def null_values_count(self, column: str) -> int:
        """Retorna a contagem de valores nulos para uma coluna."""
        self._check_column_exists(column)
        return self.dataset.filter(F.col(column).isNull()).count()
        
    def null_values_percentage(self, column: str) -> float:
        """Retorna a porcentagem de valores nulos para uma coluna."""
        null_count = self.null_values_count(column)
        total = self.total_registers
        return (null_count / total) * 100 if total > 0 else 0.0

    def total_size(self) -> str:
        """
        Retorna as dimensões do dataframe como uma string formatada: "Approx. Dimensions: X rows x Y cols".
        """
        num_rows = self.total_registers
        num_cols = len(self.dataset.columns)
        
        return f"Approx. Dimensions: {num_rows} rows x {num_cols} cols"

    def data_growth_rating(self, baseline_count: int, period: str = "day") -> float:
        """
        Calcula a taxa de crescimento (ou decaimento) em relação a um baseline.
        Requer que o Runner forneça a contagem de registros do período anterior (baseline).
        
        Args:
            baseline_count: Contagem de registros do período anterior (ex: ontem).
            period: Unidade de tempo para o relatório (ex: 'day', 'month').
            
        Returns:
            Variação percentual entre o total atual e o total anterior.
        """
        current_count = self.total_registers
        
        if baseline_count == 0:
            # Crescimento infinito ou crescimento 0 se a contagem atual for 0
            return 0.0 if current_count == 0 else float('inf') 

        # Fórmula: ((Atual - Baseline) / Baseline) * 100
        growth_rate = ((current_count - baseline_count) / baseline_count) * 100
        
        return growth_rate