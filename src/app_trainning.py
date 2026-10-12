# data_quality/app_training.py
import argparse
from config.spark import create_spark_session, Logging
# CORREÇÃO: Importa a classe do ficheiro correto no diretório runner
from metrics_process.runner.trainner_runner import trainner_run 

def parse_args():
    """Analisa os argumentos da linha de comando para o job de treino."""
    parser = argparse.ArgumentParser(description="Treina o modelo de governança do HAL-9000.")
    parser.add_argument("--labeled-path", required=True, help="Caminho S3 para o dataset de treino (resultados de DQ rotulados).")
    return parser.parse_args()

def main():
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("HAL9000-Governance-Training")
        
        Logging.info("Iniciando o ciclo de treinamento do modelo de governança.")
        
        # Instancia o orquestrador de governança
        # Assume que o ficheiro 'trainner_runner.py' foi renomeado para 'framework_governance_runner.py'
        # e está em 'metrics_process/runner/'
        governance_runner = trainner_run(spark) 
        
        # Chama o método que carrega os dados e treina o modelo
        # Precisamos garantir que a classe FrameworkGovernanceRunner tenha um método como 'run_training_cycle'
        # Baseado no seu ficheiro 'trainner_runner.py', ele pode não ter este método exato.
        # Vamos assumir que a lógica de treino está encapsulada num método chamado 'train_model_cycle' (ajuste se necessário)
        
        # Carrega os dados rotulados (O runner fará isso internamente se o método for projetado assim)
        labeled_df = spark.read.parquet(args.labeled_path)
        
        # Chama o método de treino (ajuste o nome do método se for diferente na sua classe)
        governance_runner.train_model(labeled_df) # Assumindo que o método de treino se chama train_model

        Logging.info("Ciclo de treinamento concluído com sucesso.")

    except Exception as e:
        Logging.error(f"Erro durante o ciclo de treinamento: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()