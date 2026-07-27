# app_analyses.py

import sys
import argparse
from config.spark import create_spark_session, Logging
from metrics_process.runner.analyses_runner import AnalysesRunner

def parse_args():
    """Analisa os argumentos da linha de comando para o Framework HAL-9000."""
    parser = argparse.ArgumentParser(description="Executa o Framework HAL-9000 de Qualidade de Dados.")
    
    parser.add_argument("--config-path", type=str, required=True, help="Caminho base para os arquivos de configuração JSON.")
    parser.add_argument("--datasets", type=str, required=True, help="Lista de datasets a processar, separados por vírgula.")
    parser.add_argument("--output-path", type=str, required=False, help="Caminho S3 ou local para salvar os resultados em Parquet.")
    
    return parser.parse_args()

def main():
    """Função principal que inicia a sessão Spark e o runner de análises."""
    args = parse_args()
    spark = None
    
    try:
        Logging.info("Iniciando o Framework HAL-9000 de Qualidade de Dados.")
        spark = create_spark_session("HAL9000-DQ-Engine")
        
        dataset_ids = [ds.strip() for ds in args.datasets.split(',') if ds.strip()]

        # Instancia e executa o runner com os argumentos corretos
        runner = AnalysesRunner(
            spark=spark, 
            config_base_path=args.config_path, 
            datasets=dataset_ids,
            output_path=args.output_path
        )
        runner.run_validation()
        
        Logging.info("Execução do HAL-9000 finalizada com sucesso.")

    except Exception as e:
        Logging.error(f"Erro fatal na execução do HAL-9000: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
            Logging.info("SparkSession HAL-9000 finalizada.")

if __name__ == "__main__":
    main()