from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional, Union, Dict, Any, List


class DataCompleteness:
    """
    Métricas de Completude (Completeness) para processamento PySpark distribuído.
    O cálculo é feito dinamicamente no DataFrame.
    """

    def __init__(self, df: DataFrame, column: str):
        """
        Inicializa a classe com o DataFrame e o nome da coluna a ser analisada.

        Args:
            df: DataFrame PySpark.
            column: Nome da coluna para verificar a completude.
        """
        if column not in df.columns:
            raise ValueError(f"Coluna '{column}' não encontrada no DataFrame.")
            
        self.df = df
        self.column = column
        
        # Cache das contagens base para otimização (realizado apenas uma vez)
        self.total_registers = self.df.count()
        self.filled_registers = self.df.filter(F.col(self.column).isNotNull()).count()

    def _get_total_registers(self) -> int:
        """Retorna a contagem total de registros (cached)."""
        return self.total_registers

    def _get_filled_registers(self) -> int:
        """Retorna a contagem de registros preenchidos (non-null, cached)."""
        return self.filled_registers

    def calculate(self) -> float:
        """Calcula a porcentagem de completude (registros preenchidos)."""
        total = self._get_total_registers()
        filled = self._get_filled_registers()
        
        return (filled / total) * 100 if total > 0 else 0.0
    
    # --- Métricas de Contagem e Porcentagem ---

    def count_null_fields(self) -> int:
        """Retorna a contagem de campos nulos na coluna."""
        return self._get_total_registers() - self._get_filled_registers()
    
    def percent_null_fields(self) -> float:
        """Retorna a porcentagem de campos nulos na coluna."""
        total = self._get_total_registers()
        null_fields = self.count_null_fields()
        
        return (null_fields / total) * 100 if total > 0 else 0.0
    
    # O método 'empty_fields_count/percentage' é semanticamente idêntico a 'count_null_fields/percent_null_fields'
    # no contexto do Spark, onde strings vazias são tratadas separadamente se não for explicitado.
    # Vamos manter as funções existentes, mas usando a lógica PySpark.
    empty_fields_count = count_null_fields
    empty_fields_percentage = percent_null_fields
    
    # O método 'completeness_percentage' é o mesmo que 'calculate'
    completeness_percentage = calculate
    
    def completeness_count(self) -> int:
        """Retorna a contagem de campos preenchidos na coluna."""
        return self._get_filled_registers()

    # --- Métodos de Nível de Completude e Status ---
    def is_complete(self) -> bool:
        """Verifica se a coluna está 100% preenchida."""
        return self._get_filled_registers() == self._get_total_registers()
    
    def completeness_status(self) -> str:
        """Retorna 'Complete' ou 'Incomplete'."""
        return "Complete" if self.is_complete() else "Incomplete"

    def completeness_level(self) -> str:
        """Categoriza o nível de completude (ex: Complete, Mostly Complete)."""
        completeness = self.calculate()
        if completeness == 100:
            return "Complete (100%)"
        elif completeness >= 75:
            return "Mostly Complete (75-99%)"
        elif completeness >= 50:
            return "Partially Complete (50-74%)"
        else:
            return "Incomplete (<50%)"
            
    # --- Relatórios e Sumários ---

    def completeness_summary(self) -> Dict[str, Union[int, float, str]]:
        """Gera um dicionário de resumo de completude."""
        return {
            "total_registers": self._get_total_registers(),
            "filled_registers": self._get_filled_registers(),
            "missing_registers": self.count_null_fields(),
            "completeness_percentage": self.calculate(),
            "completeness_level": self.completeness_level()
        }

    # --- Métodos Removidos/Simplificados ---
    
    # Os seguintes métodos foram removidos/comentados, pois eles dependiam de dados de histórico
    # ou de estruturas de dados complexas (listas, dicionários de campos) que não se encaixam
    # na execução de métricas por coluna:
    
    def verify_mandatory_fields(self, mandatory_columns: List[str]) -> float:
        """
        Verifica a porcentagem de registros onde TODOS os campos em 'mandatory_columns' 
        estão preenchidos (non-null).
        
        Args:
            mandatory_columns: Lista de colunas que são obrigatórias em conjunto.
            
        Retorna:
            Porcentagem de registros que atendem à regra de preenchimento.
        """
        for col in mandatory_columns:
            self._check_column_exists(col)

        total = self.total_registers
        if total == 0:
            return 0.0

        # Cria uma condição que verifica se TODAS as colunas são NOT NULL
        condition = F.lit(True)
        for col in mandatory_columns:
            condition = condition & F.col(col).isNotNull()

        # Conta os registros que satisfazem a condição
        valid_count = self.df.filter(condition).count()
        
        return (valid_count / total) * 100

    def verify_conditional_filling(self, condition_column: str, condition_value: Any, target_column: str) -> float:
        """
        Verifica a Porcentagem de Cumprimento da Regra:
        SE 'condition_column' é igual a 'condition_value', ENTÃO 'target_column' DEVE ser preenchido (non-null).
        
        Args:
            condition_column: Coluna que dispara a regra (ex: 'status').
            condition_value: Valor que dispara a regra (ex: 'ativo').
            target_column: Coluna que deve ser preenchida se a condição for satisfeita (ex: 'data_fim').
            
        Retorna:
            Porcentagem de registros que cumprem a regra de consistência condicional
            (entre o total de registros onde a condição é relevante).
        """
        self._check_column_exists(condition_column)
        self._check_column_exists(target_column)

        # 1. Definir a condição de relevância: (condition_column == condition_value)
        relevance_condition = (F.col(condition_column) == F.lit(condition_value))

        # 2. Definir a condição de sucesso: (target_column IS NOT NULL)
        success_condition = F.col(target_column).isNotNull()
        
        # O sucesso ocorre se:
        # a) O registro não é relevante (ignorado, mas contado como sucesso)
        # b) OU O registro é relevante E o campo alvo está preenchido
        compliance_condition = (~relevance_condition) | (relevance_condition & success_condition)

        # Conta o total de registros
        total_count = self.df.count()
        if total_count == 0:
            return 0.0
            
        # Conta quantos registros cumprem a regra
        compliant_count = self.df.filter(compliance_condition).count()
        
        return (compliant_count / total_count) * 100
    
    def _check_column_exists(self, column: str):
        """Verifica se a coluna existe no DataFrame."""
        if column not in self.df.columns:
            raise ValueError(f"Coluna '{column}' não encontrada no DataFrame.")
        
    
    # - Métodos de tendência (completeness_trend, completeness_over_time) - Deve ser movido para um serviço de Monitoramento/Histórico.
    # - Métodos de comparação (completeness_difference, completeness_improvement) - Deve ser movido para o Runner ou serviço de Governança.
