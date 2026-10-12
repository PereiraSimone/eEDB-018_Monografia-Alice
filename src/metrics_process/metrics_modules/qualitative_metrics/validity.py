from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any, List, Union

class DataValidity:
    """
    HAL-9000: Métricas de Validade. Focada em checar a conformidade dos dados com formatos e regras de domínio (Regex, Datas, Valores).
    """

    def __init__(self, df: DataFrame):
        """
        Inicializa a classe com o DataFrame completo.
        """
        # CORREÇÃO: Atribuição direta do DataFrame
        if df is None:
            raise ValueError("O DataFrame não pode ser None ao inicializar DataValidity.")
            
        self.df = df
        self.total_registers = self.df.count()

    def _get_compliance_percentage(self, condition: F.Column) -> float:
        """Helper distribuído para calcular o percentual de conformidade de uma condição."""
        total = self.total_registers
        if total == 0:
            return 0.0
            
        compliant_count = self.df.filter(condition).count()
        return (compliant_count / total) * 100

    def _check_column_exists(self, column: str):
        """Helper para verificar se a coluna alvo existe."""
        if column not in self.df.columns:
            raise ValueError(f"Coluna '{column}' não encontrada no DataFrame.")

    # --- 1. VALIDADE DE FORMATO (REGEX GRIP) ---

    def regex_grip_percentage(self, column: str, pattern: str) -> float:
        """Calcula a porcentagem de valores na coluna que aderem ao padrão Regex."""
        self._check_column_exists(column)
        condition = F.col(column).isNotNull() & F.col(column).rlike(pattern)
        return self._get_compliance_percentage(condition)

    def check_domain_values_percentage(self, column: str, valid_values: List[Any]) -> float:
        """Calcula a porcentagem de valores que pertencem ao conjunto de valores válidos (Domínio)."""
        self._check_column_exists(column)
        condition = F.col(column).isin(valid_values)
        return self._get_compliance_percentage(condition)

    def verify_date_format_percentage(self, column: str, date_format: str) -> float:
        """Calcula a porcentagem de strings que podem ser parseadas com sucesso no formato de data fornecido."""
        self._check_column_exists(column)
        parsed_date_col = F.to_date(F.col(column), date_format)
        condition = parsed_date_col.isNotNull()
        return self._get_compliance_percentage(condition)


    # --- VALIDAÇÃO SQL CUSTOMIZADA (NOVO MÉTODO) ---

    def execute_custom_sql_validation_percentage(self, spark: SparkSession, sql_query: str) -> float:
        """
        Executa uma Query SQL completa do Spark para calcular a porcentagem de conformidade.
        
        Args:
            spark: A instância do SparkSession.
            sql_query: Query SQL customizada. A query DEVE retornar um único valor float 
                       ou double representando a porcentagem de conformidade (0.0 a 100.0).
                       Ex: SELECT AVG(CASE WHEN condition THEN 1 ELSE 0 END) * 100 FROM __temp_view
                       
        Returns:
            O valor float de conformidade retornado pela query.
        """
        temp_view_name = "__hal_temp_view"
        
        try:
            # 1. Registra o DataFrame atual como uma View Temporária
            self.df.createOrReplaceTempView(temp_view_name)
            
            # 2. Executa a Query SQL customizada
            # A query DEVE ser escrita para usar a view temporária
            result_df = spark.sql(sql_query)
            
            # 3. Extrai o resultado (espera-se 1 linha, 1 coluna - o valor da métrica)
            result_value = result_df.collect()[0][0]
            
            # 4. Converte e retorna
            return float(result_value)

        except Exception as e:
            # Garante que a view temporária seja removida ANTES de levantar a exceção
            spark.catalog.dropTempView(temp_view_name)
            raise RuntimeError(f"Erro na execução da Query SQL Customizada: {e}")
        finally:
            # Garante que a view temporária seja removida após a execução
            spark.catalog.dropTempView(temp_view_name)
            
    # Adicionando um ALIAS para uso mais fácil no JSON
    custom_validation = execute_custom_sql_validation_percentage
