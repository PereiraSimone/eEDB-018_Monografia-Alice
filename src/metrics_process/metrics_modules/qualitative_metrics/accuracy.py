from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List, Union, Tuple
from config.spark import Logging

class DataAccuracy:
    """
    HAL-9000: Métricas de Acurácia. Focado em comparação com fontes de verdade externas ou internas e reconciliação.
    """

    def __init__(self, df: DataFrame):
        """
        Inicializa a classe com o DataFrame. O foco será na comparação de colunas.
        """
        self.df = df
        self.total_registers = self.df.count()

    def _check_columns_exist(self, columns: Union[str, List[str]]):
        """Helper para verificar se todas as colunas existem."""
        if isinstance(columns, str):
            columns = [columns]
        for col in columns:
            if col not in self.df.columns:
               Logging.error(f"Coluna '{col}' não encontrada no DataFrame.")

    # --- 1. ACURÁCIA PREDITIVA (COMPARAR DUAS COLUNAS NO MESMO DF) ---
    def calculate_accuracy(self, predictions_col: str, targets_col: str) -> float:
        """
        Calcula a acurácia pela comparação direta entre duas colunas (Predição vs. Alvo/Verdade).
        Ideal para validar modelos de ML ou valores enriquecidos.
        """
        self._check_columns_exist([predictions_col, targets_col])
        total = self.total_registers
        
        if total == 0:
            return 0.0

        # Conta as linhas onde a predição é igual ao alvo
        correct_predictions = self.df.filter(
            F.col(predictions_col) == F.col(targets_col)
        ).count()
        
        return (correct_predictions / total) * 100

    # --- 2. ACURÁCIA REFERENCIAL (COMPARAR COM LISTA EXTERNA DE VALORES) ---
    def check_external_values_percentage(self, column: str, external_values: List[Any]) -> float:
        """
        Calcula a porcentagem de valores na coluna que pertencem a um conjunto 
        de valores válidos externos (Source of Truth).
        """
        self._check_columns_exist(column)
        total = self.total_registers
        
        if total == 0:
            return 0.0
            
        # Filtra os registros onde o valor da coluna está presente na lista externa
        valid_count = self.df.filter(
            F.col(column).isin(external_values)
        ).count()
        
        return (valid_count / total) * 100

    # --- 3. ACURÁCIA DE RECONCILIAÇÃO DE FONTES (COMPARAR COM OUTRO DF) ---
    
    def compare_data_sources_similarity(self, df_other: DataFrame, join_keys: List[str]) -> float:
        """
        Calcula a similaridade de volume entre este DataFrame e outro (df_other), 
        baseado na correspondência exata das chaves de join.
        """
        self._check_columns_exist(join_keys)
        
        if not all(col in df_other.columns for col in join_keys):
            raise ValueError("As chaves de join não existem no DataFrame de comparação.")

        total_current = self.total_registers
        
        if total_current == 0:
            return 0.0

        # Realiza um JOIN interno para contar quantos registros correspondem
        matched_count = self.df.alias("current").join(
            df_other.alias("other"),
            on=join_keys,
            how="inner"
        ).count()
        
        # A similaridade é o percentual de registros do DF atual que encontrou correspondência
        return (matched_count / total_current) * 100
        
    # --- MÉTODOS DE CONSISTÊNCIA AVANÇADA (Requer UDFs) ---
    
    def consistency_check(self, column: str, rule_udf: F.udf) -> float:
        """
        Verifica a porcentagem de consistência aplicando uma função de regra customizada (UDF) 
        a cada linha da coluna.
        """
        self._check_columns_exist(column)
        total = self.total_registers
        
        if total == 0:
            return 0.0
            
        # Aplica a UDF e conta onde o resultado é True (consistente)
        consistent_count = self.df.withColumn(
            "is_consistent",
            rule_udf(F.col(column))
        ).filter(F.col("is_consistent") == True).count()
        
        return (consistent_count / total) * 100
