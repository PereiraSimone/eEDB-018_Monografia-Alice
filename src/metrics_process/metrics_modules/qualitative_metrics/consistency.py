from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List, Union, Tuple

class DataConsistency:
    """
    HAL-9000: Métricas de Consistência e Integridade. Focada em Unicidade, Domínio de Valores, Range e Chaves Estrangeiras.
    """

    def __init__(self, df: DataFrame):
        """
        Inicializa a classe com o DataFrame completo.
        """
        self.df = df
        self.total_registers = self.df.count()

    def _get_compliance_percentage(self, condition: F.Column) -> float:
        """Helper distribuído para calcular o percentual de conformidade de uma condição."""
        total = self.total_registers
        if total == 0:
            return 0.0
            
        # Conta quantos registros satisfazem a condição de forma distribuída
        compliant_count = self.df.filter(condition).count()
        return (compliant_count / total) * 100

    def _check_columns_exist(self, columns: Union[str, List[str]]):
        """Helper para verificar se todas as colunas existem."""
        if isinstance(columns, str):
            columns = [columns]
        for col in columns:
            if col not in self.df.columns:
                raise ValueError(f"Coluna '{col}' não encontrada no DataFrame.")

    # --- 1. UNICIDADE (UNIQUENESS) ---

    def uniqueness_consistency_percentage(self, columns: Union[str, List[str]]) -> float:
        """
        Calcula a porcentagem de unicidade para uma ou mais colunas (chave composta).
        """
        self._check_columns_exist(columns)
        total = self.total_registers
        
        if total == 0:
            return 0.0

        if isinstance(columns, str):
            columns = [columns]

        # Conta a quantidade de valores distintos para a chave composta
        distinct_count = self.df.select(columns).distinct().count()
        
        # A unicidade é a proporção entre distintos e o total
        return (distinct_count / total) * 100

    def duplicate_consistency_count(self, columns: Union[str, List[str]]) -> int:
        """
        Retorna a contagem de registros que são DUPLICADOS (contando a primeira ocorrência).
        """
        self._check_columns_exist(columns)
        total = self.total_registers
        
        if isinstance(columns, str):
            columns = [columns]

        # 1. Encontra a contagem de cada grupo de chaves
        df_grouped = self.df.groupBy(columns).count()
        
        # 2. Filtra grupos onde a contagem é maior que 1 (duplicados)
        # 3. Soma a contagem - 1 para cada grupo (para obter apenas as repetições)
        
        duplicate_rows_count = df_grouped.filter(F.col("count") > 1).select(
            F.sum(F.col("count") - 1)
        ).collect()[0][0]
        
        return int(duplicate_rows_count) if duplicate_rows_count is not None else 0

    # --- 2. VALOR E RANGE (RANGE AND DOMAIN CONSISTENCY) ---

    def range_consistency_percentage(self, column: str, min_value: float, max_value: float) -> float:
        """
        Calcula a porcentagem de valores que estão DENTRO do range [min_value, max_value].
        """
        self._check_columns_exist(column)

        condition = (F.col(column).isNotNull()) & \
                    (F.col(column) >= F.lit(min_value)) & \
                    (F.col(column) <= F.lit(max_value))
                    
        return self._get_compliance_percentage(condition)

    def value_consistency_percentage(self, column: str, valid_values: List[Any]) -> float:
        """
        Calcula a porcentagem de valores na coluna que pertencem ao conjunto de valores válidos (Domínio).
        """
        self._check_columns_exist(column)
        
        condition = F.col(column).isin(valid_values)
        
        return self._get_compliance_percentage(condition)

    def pattern_consistency_percentage(self, column: str, pattern: str) -> float:
        """
        Calcula a porcentagem de strings que correspondem ao padrão Regex.
        """
        self._check_columns_exist(column)
        
        # A condição verifica se o campo é uma string VÁLIDA e se corresponde ao padrão
        condition = F.col(column).rlike(pattern)
        
        return self._get_compliance_percentage(condition)

    # --- 3. CONSISTÊNCIA DE INTEGRIDADE (FOREIGN KEY / CROSS-FIELD) ---

    def foreing_key_consistency_percentage(self, primary_key_df: DataFrame, current_column: str, foreign_key_col: str) -> float:
        """
        Calcula a porcentagem de chaves na coluna atual que existem na tabela de chaves primárias (DF_other).
        """
        self._check_columns_exist(current_column)
        total_current = self.total_registers
        
        if total_current == 0:
            return 0.0

        # Renomeia a coluna da chave primária (df_other) para evitar conflito na junção
        df_keys = primary_key_df.select(F.col(foreign_key_col).alias("__key_match")).distinct()

        # Realiza um JOIN interno para contar quantas chaves correspondem
        matched_count = self.df.join(
            df_keys,
            F.col(current_column) == F.col("__key_match"),
            how="inner"
        ).count()
        
        # O percentual é a proporção de chaves que encontraram correspondência.
        return (matched_count / total_current) * 100

    def cross_field_consistency_percentage(self, spark_sql_condition: str) -> float:
        """
        Calcula a porcentagem onde a relação entre as colunas A e B é verdadeira.
        Ex: 'data_fim' > 'data_inicio'
        """
        total = self.total_registers
        
        if total == 0:
            return 0.0

        # Cria uma condição Spark SQL que pode ser aplicada diretamente
        # Ex: "col_a > col_b" (a condição é passada como string)
        compliant_count = self.df.filter(F.expr(spark_sql_condition)).count()
        
        return (compliant_count / total) * 100

    # --- MÉTODOS DE CONTROLE/LIMPEZA (Mantidos para compatibilidade) ---
    
    def null_consistency_percentage(self, column: str) -> float:
        """Alias para null_values_percentage em Bulk, focando na consistência."""
        # Esta lógica está mais adequada em DataCompleteness/Bulk, mas é mantida aqui por convenção
        from .bulk import Bulk
        bulk_analyzer = Bulk(self.df)
        return bulk_analyzer.null_values_percentage(column)

    def type_consistency_percentage(self, column: str, expected_spark_type: str) -> float:
        """
        Verifica a porcentagem de registros onde o tipo de dado real é o esperado.
        (Usa F.typeof(col) para verificar a aderência ao tipo no esquema).
        """
        self._check_columns_exist(column)
        total = self.total_registers
        
        if total == 0:
            return 0.0

        # Checa se o tipo de dado da coluna no esquema corresponde ao esperado
        # Isso é melhor para validar o SCHEMA, não o dado linha a linha, mas é mantido.
        current_type = self.df.schema[column].dataType.simpleString()
        return 100.0 if current_type == expected_spark_type else 0.0