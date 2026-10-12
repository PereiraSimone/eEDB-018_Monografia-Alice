# metrics_process/runner/analyses_runner.py

import os
import json
import subprocess
import yaml
from pyspark.sql import SparkSession
from config.spark import Logging
from metrics_process.functions.file_reader import FileReader
from metrics_process.model.dataset import Dataset
from metrics_process.functions.report_writer import ReportWriter
from metrics_process.metrics_modules.monitoring.reports import Reports
from metrics_process.functions.metrics_store_writer import export_metrics

# Importe todas as suas classes de métricas
from metrics_process.metrics_modules.qualitative_metrics.completeness import DataCompleteness
from metrics_process.metrics_modules.qualitative_metrics.validity import DataValidity
from metrics_process.metrics_modules.qualitative_metrics.consistency import DataConsistency
from metrics_process.metrics_modules.quantitative_metrics.outliers import DataOutliers
from metrics_process.metrics_modules.quantitative_metrics.distribution import DataDistribution
from metrics_process.metrics_modules.quantitative_metrics.bulk import Bulk

class AnalysesRunner:
    """
    Orquestrador de produção do HAL-9000.
    Executa um pipeline híbrido: primeiro validações críticas com Soda Core,
    depois análises aprofundadas com o framework customizado.
    """
    def __init__(self, spark: SparkSession, config_base_path: str, datasets: list, output_path: str = None):
        self.spark = spark
        self.config_base_path = config_base_path
        self.datasets_to_run = datasets
        self.output_path = output_path
        self.file_reader = FileReader(spark)
        self.reports = Reports()
        if self.output_path:
            self.report_writer = ReportWriter(spark)

    def _load_dataset_config(self, dataset_name: str) -> dict:
        config_path = os.path.join(self.config_base_path, f"{dataset_name}.json")
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            Logging.error(f"Falha ao carregar configuração para '{dataset_name}' em {config_path}: {e}")
            return None

    def _run_soda_checks(self, dataset_name: str, temp_view_name: str) -> bool:
        """Executa os testes declarativos com Soda Core como um portão de qualidade."""
        Logging.info(f"--- FASE 1: Executando Validações Críticas com Soda Core para '{temp_view_name}' ---")
        
        soda_checks_path = f"/app/soda_checks/{dataset_name}_checks.yml"
        if not os.path.exists(soda_checks_path):
            Logging.warning(f"Arquivo de checks do Soda não encontrado em {soda_checks_path}. Pulando esta etapa.")
            return True

        spark_session_name = self.spark.conf.get("spark.app.name")
        soda_config_path = "/app/soda_config.yaml"
        soda_config = {
            'data_source spark_dq': {
                'type': 'spark',
                'spark_session': spark_session_name
            }
        }
        with open(soda_config_path, 'w') as f:
            yaml.dump(soda_config, f)
        
        command = ["soda", "scan", "-d", "spark_dq", "-c", soda_config_path, soda_checks_path]

        try:
            # Usamos subprocess.run para executar o comando
            result = subprocess.run(command, capture_output=True, text=True, check=False) # check=False para não dar erro se o soda falhar
            
            # Mesmo que o processo termine, pode haver erros no output do soda
            if result.returncode != 0:
                Logging.error(f"ERRO: Soda Scan terminou com código de saída {result.returncode}.")
                Logging.error(f"Saída de Erro do Soda (stderr):\n{result.stderr}")

            # Tentamos ler o output JSON independentemente do código de retorno
            scan_output = json.loads(result.stdout)
            export_metrics(f'soda_checks_{dataset_name}', scan_output)
            
            if scan_output.get("hasFailures") or scan_output.get("hasErrors"):
                Logging.error(f"ERRO: Validações críticas do Soda Core falharam para {dataset_name}. Pipeline interrompido.")
                Logging.error(json.dumps(scan_output, indent=2))
                return False
            
            Logging.info(f"SUCESSO: Validações críticas do Soda Core passaram para {dataset_name}.")
            return True
        except (json.JSONDecodeError, KeyError) as e:
            Logging.error(f"ERRO CRÍTICO ao processar a saída do Soda Scan para {dataset_name}: {e}")
            Logging.error(f"Saída Bruta do Soda (stdout):\n{result.stdout}")
            return False

    def _execute_advanced_validations(self, df, validations: list, source_name: str):
        Logging.info(f"--- FASE 2: Executando Validações Avançadas Customizadas para '{source_name}' ---")
        # ... (O resto da função continua igual à versão anterior)
        if df is None or not validations:
            return

        for rule in validations:
            if not rule.get("active", True): continue

            metric_type = rule.get("validation_type")
            metric_name = rule.get("metric")
            column = rule.get("column")
            args = rule.get("args", {})

            try:
                metric_class_map = {
                    "DataCompleteness": DataCompleteness(df, column) if column else None,
                    "DataValidity": DataValidity(df),
                    "DataConsistency": DataConsistency(df),
                    "DataOutliers": DataOutliers(df, column) if column else None,
                    "DataDistribution": DataDistribution(df, column) if column else None,
                    "Bulk": Bulk(df)
                }
                metric_instance = metric_class_map.get(metric_type)
                if not metric_instance:
                    raise AttributeError(f"Tipo de validação desconhecido: '{metric_type}'")
                
                metric_method = getattr(metric_instance, metric_name)
                result_value = metric_method(**args) if args else metric_method()
                
                report_content = {"source": source_name, "column": column or "N/A", "metric": f"{metric_type}.{metric_name}", "value": result_value, "status": "SUCCESS"}
                self.reports.add_report({"content": report_content})
                Logging.info(f"Métrica '{metric_name}' na coluna '{column}' executada. Resultado: {result_value}")
            except Exception as e:
                Logging.error(f"Falha na métrica '{metric_name}' para '{column}'. Erro: {e}", exc_info=True)
                self.reports.add_report({"content": {"source": source_name, "column": column, "metric": f"{metric_type}.{metric_name}", "value": None, "status": "FAIL", "error_message": str(e)}})


    def run_validation(self):
        Logging.info(f"Iniciando validação de produção para os datasets: {self.datasets_to_run}")

        for dataset_name in self.datasets_to_run:
            self.reports.clear_reports()
            Logging.info(f"================= Processando dataset: {dataset_name} =================")
            
            config = self._load_dataset_config(dataset_name)
            if not config: continue

            dataset_obj = Dataset.from_dict(config)
            df = self.file_reader.read(dataset_obj)
            if df is None: continue

            temp_view_name = f"{dataset_name}_view"
            df.createOrReplaceTempView(temp_view_name)
            Logging.info(f"DataFrame carregado e visão temporária '{temp_view_name}' criada.")

            soda_passed = self._run_soda_checks(dataset_name, temp_view_name)

            if soda_passed:
                self._execute_advanced_validations(df, config.get("validations", []), dataset_name)
            
            Logging.info(self.reports.generate_text_report())
            if self.output_path:
                dataset_output_path = os.path.join(self.output_path, dataset_name)
                self.report_writer.write_as_parquet(self.reports, dataset_output_path)
            
            df.unpersist()
            self.spark.catalog.dropTempView(temp_view_name)

        Logging.info("Todos os datasets foram processados.")