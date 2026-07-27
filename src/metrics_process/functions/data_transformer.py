# metrics_process/functions/data_transformer.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, List, Any
from config.spark import Logging
# Importa o nosso novo módulo de window functions
from metrics_process.functions import window_functions

class DataTransformer:
    """
    HAL-9000: Motor de Transformação de Dados.
    Encapsula operações de ETL e limpeza de DataFrames no Spark de forma declarativa.
    """
    def __init__(self):
        pass

    # --- MÉTODOS DE EXPRESSÃO E LÓGICA ---
    def apply_expressions(self, df: DataFrame, expressions: Dict[str, str]) -> DataFrame:
        if not expressions: return df
        transformed_df = df
        Logging.info(f"Aplicando {len(expressions)} expressões customizadas.")
        for col_name, expr_str in expressions.items():
            try:
                transformed_df = transformed_df.withColumn(col_name, F.expr(expr_str))
            except Exception as e:
                Logging.error(f"Falha ao aplicar a expressão para a coluna '{col_name}': '{expr_str}'. Erro: {e}")
                continue
        return transformed_df

    def with_conditional_column(self, df: DataFrame, new_col_name: str, conditions: Dict[str, Any], otherwise_value: Any = None) -> DataFrame:
        if not new_col_name or not conditions: return df
        Logging.info(f"Criando coluna condicional '{new_col_name}'.")
        when_chain = None
        for condition, value in conditions.items():
            when_chain = F.when(F.expr(condition), value) if when_chain is None else when_chain.when(F.expr(condition), value)
        if otherwise_value is not None:
            when_chain = when_chain.otherwise(otherwise_value)
        return df.withColumn(new_col_name, when_chain)

    # --- MÉTODOS DE MANIPULAÇÃO DE ESQUEMA ---
    def rename_columns(self, df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
        if not rename_map: return df
        return df.withColumnsRenamed(rename_map)

    def cast_columns(self, df: DataFrame, cast_map: Dict[str, str]) -> DataFrame:
        if not cast_map: return df
        casted_df = df
        for col_name, new_type in cast_map.items():
            if col_name in casted_df.columns:
                casted_df = casted_df.withColumn(col_name, F.col(col_name).cast(new_type))
        return casted_df

    def drop_columns(self, df: DataFrame, columns_to_drop: List[str]) -> DataFrame:
        if not columns_to_drop: return df
        return df.drop(*columns_to_drop)

    # --- MÉTODOS DE LIMPEZA E FILTRAGEM DE DADOS ---
    def handle_nulls(self, df: DataFrame, fill_strategy: Dict[str, Any]) -> DataFrame:
        strategy = fill_strategy.get("strategy", "").lower()
        columns = fill_strategy.get("columns", [])
        if not columns or not strategy: return df
        if strategy == "literal":
            return df.fillna(fill_strategy.get("value"), subset=columns)
        elif strategy in ["mean", "median", "mode"]:
            from pyspark.ml.feature import Imputer
            imputer = Imputer(inputCols=columns, outputCols=columns).setStrategy(strategy)
            return imputer.fit(df).transform(df)
        return df

    def trim_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        if not columns: return df
        transformed_df = df
        for col_name in columns:
            if col_name in transformed_df.columns:
                transformed_df = transformed_df.withColumn(col_name, F.trim(F.col(col_name)))
        return transformed_df

    def change_case(self, df: DataFrame, case_type: str, columns: List[str]) -> DataFrame:
        if not columns or not case_type: return df
        case_func = F.upper if case_type.lower() == 'upper' else F.lower
        transformed_df = df
        for col_name in columns:
            if col_name in transformed_df.columns:
                transformed_df = transformed_df.withColumn(col_name, case_func(F.col(col_name)))
        return transformed_df

    def filter_by_condition(self, df: DataFrame, condition: str) -> DataFrame:
        if not condition: return df
        return df.filter(condition)

    def drop_duplicates(self, df: DataFrame, columns: List[str] = None) -> DataFrame:
        return df.dropDuplicates(subset=columns) if columns else df.dropDuplicates()

    def deduplicate_by_key(self, df: DataFrame, key: Dict[str, Any]) -> DataFrame:
        """
        [REFATORADO] Desduplica um DataFrame, mantendo apenas o primeiro registo para uma chave.
        A lógica da Window Function foi isolada em 'window_functions.py'.
        """
        partition_by_cols = key.get("partition_by_cols")
        order_by_col = key.get("order_by_col")
        ascending = key.get("ascending", False)

        if not partition_by_cols or not order_by_col:
            Logging.error("Para desduplicação, o 'key' deve conter 'partition_by_cols' e 'order_by_col'.")
            return df
            
        try:
            Logging.info(f"Desduplicando por chave {partition_by_cols}, ordenado por {order_by_col} {'ASC' if ascending else 'DESC'}.")
            ranked_df = window_functions.rank_rows(df, partition_by_cols, order_by_col, ascending)
            return ranked_df.filter(F.col("row_rank") == 1).drop("row_rank")
        except Exception as e:
            Logging.error(f"Falha ao desduplicar por chave: {e}")
            return df

    # --- MÉTODOS DE FEATURE ENGINEERING E ENRIQUECIMENTO ---
    def hash_column(self, df: DataFrame, new_col_name: str, source_col: str, algorithm: str = 'sha2') -> DataFrame:
        if not all([new_col_name, source_col, algorithm]): return df
        return df.withColumn(new_col_name, F.expr(f"{algorithm}(CAST({source_col} AS STRING))"))

    def extract_from_regex(self, df: DataFrame, new_col_name: str, source_col: str, pattern: str, group_index: int = 1) -> DataFrame:
        if not all([new_col_name, source_col, pattern]): return df
        return df.withColumn(new_col_name, F.regexp_extract(F.col(source_col), pattern, group_index))
        
    def with_date_column(self, df: DataFrame, new_col_name: str, from_col: str, date_format: str) -> DataFrame:
        if not all([new_col_name, from_col, date_format]): return df
        return df.withColumn(new_col_name, F.to_date(F.col(from_col), date_format))

    def join_with(self, df: DataFrame, spark: SparkSession, other_dataset_config: Dict[str, Any], join_cols: List[str], join_type: str = 'inner') -> DataFrame:
        # (código inalterado)
        pass