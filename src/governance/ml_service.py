# metrics_process/governance/ml_service.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml.stat import Summarizer
from typing import List, Dict, Any
import numpy as np

class MLService:
    """
    Serviço que implementa modelos de Machine Learning (Runner de Governança)
    para detecção de anomalias, validação avançada e monitoramento de drift.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.ml_models = {}
        # Usado para modelos que precisam ser persistidos e reusados (ex: OCSVM, IsolationForest)
        self.anomaly_models = {} 

    # --- Pré-processamento Comum ---

    def _prepare_features(self, df: DataFrame, feature_cols: List[str], output_col: str = "features") -> DataFrame:
        """Helper para montar o vetor de features para modelos MLlib."""
        assembler = VectorAssembler(inputCols=feature_cols, outputCol=output_col)
        return assembler.transform(df)

    # --- 1. Detecção de Anomalias (Outliers Avançados) ---
    
    def detect_anomalies_kmeans(self, df: DataFrame, feature_cols: List[str], k: int = 5, model_id: str = 'kmeans') -> DataFrame:
        """
        Usa K-Means para agrupar dados e identificar a distância de cada ponto
        aos centros de cluster (método robusto para anomalias multivariadas).
        """
        df_assembled = self._prepare_features(df, feature_cols)
        
        # O K-Means em si não rotula anomalias, mas sim a distância:
        kmeans = KMeans(featuresCol="features", k=k, seed=1)
        model = kmeans.fit(df_assembled)
        self.anomaly_models[model_id] = model
        
        # Predição e cálculo de distância até o centro do cluster
        def calculate_distance(row):
             # Simulação: Na implementação real, isso exigiria uma UDF complexa
             # ou um cálculo otimizado do PySpark.
            return float(np.linalg.norm(row['features'].toArray() - model.clusterCenters()[row['prediction']]))
        
        # Retorna o DataFrame com a distância (base para detecção de anomalias por IQR/limiar)
        return df_assembled.withColumn("distance_to_center", F.lit(None)) # Placeholder para UDF/cálculo distribuído
    
    def detect_anomalies_one_class_svm(self, df: DataFrame, feature_cols: List[str], nu: float = 0.05, model_id: str = 'ocsvm') -> DataFrame:
        """
        Modelos como OCSVM (Isolation Forest é mais comum no PySpark) são
        usados para detectar anomalias aprendendo a fronteira de dados 'normais'.
        """
        # Nota: O PySpark MLlib não inclui nativamente Isolation Forest ou OCSVM.
        # Seria necessário usar UDFs com Scikit-learn (ineficiente) ou uma biblioteca
        # de terceiros. Usaremos um placeholder que, na prática, é um requisito.
        
        print("Implementação OCSVM/Isolation Forest requer bibliotecas externas ou UDFs otimizadas.")
        
        # placeholder para manter a estrutura e alertar o engenheiro
        return df.withColumn("is_anomaly_ocsvm", F.lit(None)) 

    # --- 2. Consistência e Validade (Classificação e Previsão) ---
    
    def classify_conditional_validity(self, df: DataFrame, feature_cols: List[str], target_col: str, model_id: str = 'conditional_lr') -> DataFrame:
        """
        Usa Regressão Logística ou Árvore de Decisão para prever a validade/valor de 
        uma coluna (Target) baseado em Features, checando se o valor real é plausível.
        
        Args:
            df: DataFrame PySpark que deve conter a coluna rotulada ('target_col').
            feature_cols: Colunas a serem usadas como preditores.
            target_col: Coluna que está sendo validada/prevista.
        """
        # 1. Preparação (inclui StringIndexer/OneHotEncoder se necessário)
        df_processed = self._prepare_features(df, feature_cols + [target_col], output_col="features")
        
        # 2. Treinamento
        lr = LogisticRegression(featuresCol="features", labelCol=target_col)
        model = lr.fit(df_processed)
        self.ml_models[model_id] = model

        # 3. Predição
        predictions = model.transform(df_processed)
        
        # 4. Avaliação de Plausibilidade: Se a probabilidade de a previsão ser
        # o valor real for baixa, marca como inconsistente.
        # A coluna 'prediction' indica o valor mais provável.
        return predictions.withColumn("is_inconsistent_ml", F.when(F.col(target_col) != F.col("prediction"), True).otherwise(False))
    
    # --- 3. Imputação de Dados (Tratamento de Nulos Inteligente) ---

    def impute_missing_values(self, df: DataFrame, strategy: str = "median", imputation_cols: List[str] = []) -> DataFrame:
        """
        Preenche valores nulos em colunas numéricas usando PySpark Imputer (distribuído).
        """
        if not imputation_cols:
            return df
            
        imputer = Imputer(
            inputCols=imputation_cols, 
            outputCols=[f"{col}_imputed" for col in imputation_cols]
        ).setStrategy(strategy)
        
        model = imputer.fit(df)
        df_imputed = model.transform(df)
        
        # O modelo Imputer faz o cálculo de média/mediana/moda distribuído, garantindo escalabilidade.
        return df_imputed
        
    # --- 4. Monitoramento Temporal e Drift (Mudança de Distribuição) ---
    
    def calculate_psi(self, df_current: DataFrame, df_baseline: DataFrame, column: str, bins: int = 10) -> float:
        """
        Calcula o Population Stability Index (PSI) para detectar Data Drift.
        PSI > 0.25 (problema severo), PSI > 0.1 (alerta).
        """
        
        # Para ser distribuído, requer a criação de buckets (quantiles) no df_baseline
        # e a contagem de frequência nos dois DataFrames.
        
        # 1. Geração de Quantiles no Baseline para definir os 'bins'
        boundaries = df_baseline.approxQuantile(column, [i/bins for i in range(1, bins)], 0.01)
        
        # 2. Contagem de frequência em cada bin (current e baseline)
        
        # 3. Cálculo do PSI
        # PSI = SUM [ (% Atual - % Base) * LN(% Atual / % Base) ]
        
        # Código complexo de PySpark omitido para fins de esboço.
        # Aqui, retornamos um valor que simula o resultado.
        
        print(f"Calculando PSI para a coluna {column}...")
        return 0.15 # Exemplo de resultado (alerta)

    def compare_distribution_ks(self, df_current: DataFrame, df_baseline: DataFrame, column: str) -> float:
        """
        Aplica o teste de Kolmogorov-Smirnov (KS) para ver se duas amostras
        vieram da mesma distribuição (requer amostra pequena ou MLlib).
        """
        # Nota: O PySpark tem funções de KS, mas requer que o dado seja passado como
        # uma lista (pequena) ou através de um processo de amostragem complexo.
        
        # Simplificando a chamada para o PySpark:
        from pyspark.ml.stat import KolmogorovSmirnovTest
        
        # Este método não é trivial em PySpark distribuído, mas a intenção é checar a distribuição.
        print(f"Executando teste KS para {column}...")
        return 0.05 # Exemplo de valor-p