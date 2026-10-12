# metrics_process/functions/window_functions.py

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import List, Optional
from config.spark import Logging

def _create_window_spec(partition_by_cols: List[str], order_by_col: Optional[str] = None, ascending: bool = False) -> Window:
    """
    (Helper Privado) Cria uma especificação de janela (WindowSpec) reutilizável.
    """
    if not partition_by_cols:
        raise ValueError("A especificação da janela requer pelo menos uma coluna de partição.")
        
    window = Window.partitionBy(*[F.col(c) for c in partition_by_cols])
    
    if order_by_col:
        order_func = F.col(order_by_col).asc() if ascending else F.col(order_by_col).desc()
        window = window.orderBy(order_func)
        
    return window

def add_rank(df: DataFrame, new_col_name: str, partition_by_cols: List[str], order_by_col: str, ascending: bool = False, dense: bool = False) -> DataFrame:
    """
    Adiciona uma coluna de rank (row_number, rank, or dense_rank) ao DataFrame.

    Args:
        df (DataFrame): O DataFrame de entrada.
        new_col_name (str): O nome da nova coluna de rank.
        partition_by_cols (List[str]): As colunas para particionar (a chave de negócio).
        order_by_col (str): A coluna para ordenar e determinar o rank.
        ascending (bool): A direção da ordenação. False para descendente (padrão).
        dense (bool): Se True, usa dense_rank(). Se False (padrão), usa rank(). Para row_number, use a função rank_rows.

    Returns:
        DataFrame: O DataFrame com a coluna de rank adicionada.
    """
    window_spec = _create_window_spec(partition_by_cols, order_by_col, ascending)
    rank_function = F.dense_rank() if dense else F.rank()
    return df.withColumn(new_col_name, rank_function.over(window_spec))

def add_row_number(df: DataFrame, new_col_name: str, partition_by_cols: List[str], order_by_col: str, ascending: bool = False) -> DataFrame:
    """Adiciona uma coluna de numeração sequencial (row_number) dentro de cada partição."""
    window_spec = _create_window_spec(partition_by_cols, order_by_col, ascending)
    return df.withColumn(new_col_name, F.row_number().over(window_spec))

def add_window_aggregate(df: DataFrame, new_col_name: str, agg_func: str, agg_col: str, partition_by_cols: List[str], order_by_col: Optional[str] = None, ascending: bool = False) -> DataFrame:
    """
    [NOVO E PODEROSO] Adiciona uma coluna com uma agregação sobre uma janela (ex: soma acumulada, média móvel).

    Args:
        df (DataFrame): O DataFrame de entrada.
        new_col_name (str): O nome da nova coluna de agregação.
        agg_func (str): A função de agregação a ser aplicada (ex: 'sum', 'avg', 'max', 'min', 'count').
        agg_col (str): A coluna sobre a qual a agregação será calculada.
        partition_by_cols (List[str]): As colunas para particionar a janela.
        order_by_col (Optional[str]): Se fornecido, a agregação será cumulativa (ex: soma acumulada).
                                      Se não, a agregação será sobre toda a partição.
        ascending (bool): A direção da ordenação para agregações cumulativas.

    Returns:
        DataFrame: O DataFrame com a nova coluna de agregação.
    """
    spark_agg_func = getattr(F, agg_func, None)
    if not spark_agg_func:
        raise AttributeError(f"A função de agregação '{agg_func}' não é válida no PySpark Functions.")

    # Se uma ordenação é definida, a janela por padrão vai do início até a linha atual.
    # Se não há ordenação, a janela cobre toda a partição.
    window_spec = _create_window_spec(partition_by_cols, order_by_col, ascending)
    if order_by_col:
        # Define o frame da janela para ser cumulativo
        window_spec = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return df.withColumn(new_col_name, spark_agg_func(F.col(agg_col)).over(window_spec))

def add_lag_or_lead(df: DataFrame, new_col_name: str, source_col: str, offset: int, partition_by_cols: List[str], order_by_col: str, ascending: bool = False) -> DataFrame:
    """
    [NOVO E PODEROSO] Adiciona uma coluna com o valor de uma linha anterior (lag) ou posterior (lead).

    Args:
        df (DataFrame): O DataFrame de entrada.
        new_col_name (str): O nome da nova coluna.
        source_col (str): A coluna da qual obter o valor.
        offset (int): O número de linhas a olhar para trás (offset > 0 para lag) ou para a frente (offset < 0 para lead).
        partition_by_cols (List[str]): As colunas para particionar a janela.
        order_by_col (str): A coluna que define a sequência (geralmente uma data).
        ascending (bool): A direção da ordenação.

    Returns:
        DataFrame: O DataFrame com a nova coluna de lag/lead.
    """
    window_spec = _create_window_spec(partition_by_cols, order_by_col, ascending)
    
    # Usamos o sinal do offset para decidir entre lag e lead
    if offset > 0:
        func = F.lag(F.col(source_col), offset)
    elif offset < 0:
        func = F.lead(F.col(source_col), abs(offset))
    else: # offset == 0
        return df.withColumn(new_col_name, F.col(source_col))
        
    return df.withColumn(new_col_name, func.over(window_spec))