# metrics_process/governance/framework_accuracy_validator.py

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql import DataFrame
from typing import List

class FrameworkAccuracyValidator:
    """
    Usa um modelo de ML para validar a acurácia do próprio framework de DQ, 
    ajudando a calibrar thresholds e regras.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model = None
        self.assembler = None
        
        # --- LÓGICA MOVIDA PARA CÁ ---
        # A própria classe define as features que usará para o treinamento.
        self.features = [
            "value", # O valor da métrica original (ex: percent_null_fields)
            "metric_execution_time", # O tempo de execução da métrica
            # Adicione outras colunas de metadados do report se forem úteis
            # Ex: "threshold_fail" (poderia ser convertido para numérico)
        ]
        
        # A configuração das features agora é uma etapa interna da inicialização.
        self._setup_features()

    def _setup_features(self):
        """(Privado) Define e prepara as features para o ML."""
        if not self.features:
            raise ValueError("A lista de features não pode estar vazia.")
        
        # Transforma colunas de métricas em um vetor de features
        self.assembler = VectorAssembler(inputCols=self.features, outputCol="features_vector")

    def train_model(self, labeled_results_df: DataFrame):
        """
        Treina o modelo de classificação.
        
        labeled_results_df deve conter as colunas de 'features' e a coluna 'true_label'.
        """
        
        if not self.assembler:
            raise Exception("O VectorAssembler não foi inicializado. Chame _setup_features() no construtor.")
        
        # 1. Preparação dos dados
        data = self.assembler.transform(labeled_results_df)
        
        # 2. Divisão para treino/teste
        (trainingData, testData) = data.randomSplit([0.7, 0.3], seed=42)
        
        # 3. Treinamento do modelo
        rf = RandomForestClassifier(labelCol="true_label", featuresCol="features_vector", numTrees=10)
        self.model = rf.fit(trainingData)
        
        # 4. Avaliação (Opcional, mas crucial para evolução)
        predictions = self.model.transform(testData)
        
        # Você pode adicionar um ClassificationEvaluator aqui para logar a acurácia, F1-score, etc.
        # from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        # evaluator = MulticlassClassificationEvaluator(labelCol="true_label", predictionCol="prediction", metricName="accuracy")
        # accuracy = evaluator.evaluate(predictions)
        # print(f"Acurácia do modelo de governança no set de teste: {accuracy}")
        
        print("Modelo de calibração treinado com sucesso.")

    def predict_accuracy(self, new_results_df: DataFrame) -> DataFrame:
        """Aplica o modelo para prever a 'verdadeira' acurácia de novos resultados."""
        if not self.model or not self.assembler:
            raise Exception("O modelo precisa ser treinado primeiro.")
            
        data = self.assembler.transform(new_results_df)
        predictions = self.model.transform(data)
        
        return predictions.select("*", "prediction")