import json
import os
import sys

# Caminho do contrato JSON que vamos validar
SCHEMA_PATH = "src/resources/schemas/schema_operadoras_ativas.json"
COLUNAS_OBRIGATORIAS = ["cnpj_basico", "razao_social"]

import json
import os
import sys

# Caminho do contrato JSON que vamos validar
SCHEMA_PATH = "src/resources/schemas/schema_operadoras_ativas.json"
COLUNAS_OBRIGATORIAS = ["cnpj_basico", "razao_social"]

def validar_json():
    print(f"Validando o contrato: {SCHEMA_PATH}")
    
    if not os.path.exists(SCHEMA_PATH):
        print(f"❌ ERRO: O ficheiro de contrato {SCHEMA_PATH} não foi encontrado.")
        sys.exit(1)

    try:
        with open(SCHEMA_PATH, 'r') as f:
            schema = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ ERRO SINTÁTICO: O arquivo JSON está malformado. Detalhes: {e}")
        sys.exit(1)

    # Lógica inteligente para extrair o nome das colunas dependendo do formato do JSON
    colunas_presentes = []
    
    if isinstance(schema, list):
        # Formato: [{"name": "cnpj_basico", "type": "string"}, ...]
        if len(schema) > 0 and isinstance(schema[0], dict):
            colunas_presentes = [campo.get("name") for campo in schema if "name" in campo]
        # Formato: ["cnpj_basico", "razao_social"]
        else:
            colunas_presentes = schema
            
    elif isinstance(schema, dict):
        # Formato PySpark/Avro: {"fields": [{"name": "cnpj_basico"}, ...]}
        if "fields" in schema and isinstance(schema["fields"], list):
            colunas_presentes = [campo.get("name") for campo in schema["fields"] if "name" in campo]
        # Formato dicionário simples: {"cnpj_basico": "string", "razao_social": "string"}
        else:
            colunas_presentes = list(schema.keys())

    # Verifica a Quebra de Contrato
    for col_obrigatoria in COLUNAS_OBRIGATORIAS:
        if col_obrigatoria not in colunas_presentes:
            print(f"❌ QUEBRA DE CONTRATO DETECTADA: A coluna obrigatória '{col_obrigatoria}' foi removida ou alterada!")
            print(f"Colunas encontradas no ficheiro: {colunas_presentes}")
            sys.exit(1) 

    print("✅ Contrato validado com sucesso. Nenhuma quebra de integridade estrutural.")
    sys.exit(0)

if __name__ == "__main__":
    validar_json()