import json
import os
import sys

SCHEMA_PATH = "src/resources/schemas/schema_operadoras_ativas.json"
COLUNAS_OBRIGATORIAS = ["cnpj_basico", "razao_social"] # Regra de negócio inegociável

def validar_json():
    print(f"Validando o contrato: {SCHEMA_PATH}")
    
    # 1. Verifica se o JSON é válido (Sintaxe)
    try:
        with open(SCHEMA_PATH, 'r') as f:
            schema = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ ERRO SINTÁTICO: O arquivo JSON está malformado. Detalhes: {e}")
        sys.exit(1) # Força a quebra do GitHub Actions

    # 2. Verifica se a alteração removeu colunas críticas (Quebra de Contrato)
    # Supondo que o JSON seja uma lista de dicionários [{"name": "cnpj_basico", "type": "string"}, ...]
    colunas_presentes = [campo.get("name") for campo in schema]
    
    for col_obrigatoria in COLUNAS_OBRIGATORIAS:
        if col_obrigatoria not in colunas_presentes:
            print(f"❌ QUEBRA DE CONTRATO DETECTADA: A coluna obrigatória '{col_obrigatoria}' foi removida ou alterada!")
            sys.exit(1) # O Exit Code 1 avisa ao GitHub Actions que o Deploy deve falhar

    print("✅ Contrato validado com sucesso. Nenhuma quebra de integridade estrutural.")
    sys.exit(0)

if __name__ == "__main__":
    validar_json()