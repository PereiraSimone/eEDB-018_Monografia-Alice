import subprocess
import re
import sys
import time

def _get_application_attempt_id(application_id):
    """
    Encontra o ID da tentativa de aplicação mais recente para um dado application_id.
    O YARN cria uma ou mais "tentativas" para cada aplicação.
    """
    result = subprocess.run(
        ["yarn", "applicationattempt", "-list", application_id],
        capture_output=True,
        text=True,
        check=True
    )
    # A regex busca pelo ID da tentativa (appattempt_...) na saída do comando
    match = re.search(r'(appattempt_\d+_\d+_\d+)', result.stdout)
    if not match:
        raise ValueError(f"Não foi possível encontrar uma tentativa de aplicação para {application_id}")
    return match.group(1)

def _get_container_id(application_attempt_id):
    """
    Encontra o ID do contêiner do ApplicationMaster (onde o driver Spark roda).
    """
    result = subprocess.run(
        ["yarn", "container", "-list", application_attempt_id],
        capture_output=True,
        text=True,
        check=True
    )
    # A regex busca pelo ID do contêiner (container_...)
    match = re.search(r'(container_\d+_\d+_\d+_\d+)', result.stdout)
    if not match:
        raise ValueError(f"Não foi possível encontrar um contêiner para a tentativa {application_attempt_id}")
    return match.group(0)

def get_yarn_logs(application_id):
    """
    Busca e retorna os logs completos (stdout e stderr) para o contêiner
    do driver de uma aplicação Spark no YARN.
    """
    try:
        app_attempt_id = _get_application_attempt_id(application_id)
        container_id = _get_container_id(app_attempt_id)
        
        print(f"--- Buscando logs para App: {application_id}, Contêiner: {container_id} ---")
        
        result = subprocess.run(
            ["yarn", "logs", "-applicationId", application_id, "-containerId", container_id],
            capture_output=True,
            text=True,
            check=True
        )
        
        return result.stdout + "\n" + result.stderr

    except (subprocess.CalledProcessError, ValueError) as e:
        return f"Erro ao buscar logs: {e}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python get_yarn_logs.py <application_id>")
        sys.exit(1)
        
    app_id = sys.argv[1]
    logs = get_yarn_logs(app_id)
    print(logs)