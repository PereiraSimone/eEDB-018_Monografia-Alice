import json
import datetime

def export_metrics(check_name: str, results: dict):
    """
    Padroniza e exporta os resultados das validações para um "Metrics Store".
    Nesta versão, ele apenas imprime o JSON no console.
    """
    
    # Cria um payload padronizado
    metric_payload = {
        "check_name": check_name,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "status": "success", # Pode ser 'failure', 'warning' etc.
        "results": results
    }
    
    # Em produção, você enviaria este payload para:
    # - Um tópico do Kafka
    # - Uma tabela no BigQuery/Snowflake/Redshift
    # - Um sistema de monitoramento como o Datadog
    
    # Por enquanto, imprimimos no console de forma estruturada.
    print("\n--- METRICS STORE PAYLOAD ---")
    print(json.dumps(metric_payload, indent=2))
    print("---------------------------\n")