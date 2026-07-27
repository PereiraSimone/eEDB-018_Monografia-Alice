# config/metrics_process/config.py

import json
from typing import Dict, Any

class Config:
    def __init__(self, config_path: str):
        self.config_path = config_path

    def load_config(self) -> Dict[str, Any]:
        if not self.config_path:
            raise ValueError("O caminho do arquivo de configuração não foi fornecido.")
        
        try:
            # ==============================================================================
            # CORREÇÃO: Simplificamos a lógica para ler apenas arquivos locais,
            # que é o nosso caso de uso dentro do contêiner.
            # O 'import requests' e a lógica de HTTP foram removidos.
            # ==============================================================================
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = json.load(file)
            
            self._validate_structure(config)
            return config

        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Erro ao ler o arquivo JSON: JSON mal formatado: {e}")
        except Exception as e:
            raise Exception(f"Erro ao carregar a configuração: {e}")

    def _validate_structure(self, config: Dict[str, Any]):
            required_keys = ["dataset", "source_path", "format", "validations"]
            for key in required_keys:
                if key not in config:
                    raise ValueError(f"Chave obrigatória '{key}' ausente na configuração do dataset.")
            if not isinstance(config.get("validations"), list):
                raise ValueError("A seção 'validations' deve ser uma lista de regras.")