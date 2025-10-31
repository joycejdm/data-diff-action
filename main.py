# main.py (VERSÃO v0.2.0 - SLIM CI)
import os
import requests
import json
import snowflake.connector
import subprocess
import sys
import zipfile # Para descompactar o artefacto
import io # Para lidar com o zip em memória

# --- Função Helper para Postar Comentário (Corrigida) ---
def post_comment(message_body):
    print(f"A postar comentário...")
    try:
        token = os.environ['INPUT_GITHUB_TOKEN'] # Espera 'INPUT_GITHUB_TOKEN'
        event_path = os.environ['GITHUB_EVENT_PATH']
        with open(event_path) as f:
            event_data = json.load(f)
        comments_url = event_data['pull_request']['comments_url']
        payload = {'body': message_body}
        headers = { 'Authorization': f'token {token}', 'Accept': 'application/vnd.github.v3+json' }
        response = requests.post(comments_url, json=payload, headers=headers)
        if response.status_code != 201:
            print(f"Erro ao postar comentário: {response.text}")
    except Exception as e:
        print(f"Falha crítica ao tentar postar comentário: {e}")

# --- Função Helper para Rodar Comandos (Corrigida) ---
def run_command(command, cwd_dir, profiles_dir):
    print(f"Executando: {' '.join(command)} (no diretório: {cwd_dir})")
    command.extend(["--profiles-dir", profiles_dir])
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', cwd=cwd_dir)
    if result.returncode != 0:
        print("--- ERRO no Subprocess ---"); print(result.stdout); print(result.stderr)
        raise Exception(f"Comando falhou: {' '.join(command)}. Veja o log da Action para detalhes.")
    print("--- Saída do Subprocess ---"); print(result.stdout)
    return result.stdout

# --- Função Helper para Criar o profiles.yml (Corrigida) ---
def create_profiles_yml(profiles_dir, sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema):
    print("A criar profiles.yml temporário...")
    os.makedirs(profiles_dir, exist_ok=True)
    profiles_yml_content = f"""
    default:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: {sf_account} # A conta completa (ex: ...sa-east-1.aws)
          user: {sf_user}
          password: {sf_password}
          role: {sf_role}
          warehouse: {sf_warehouse}
          database: {sf_database}
          schema: {clone_schema}
          threads: 1
    """
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        f.write(profiles_yml_content)
    print("profiles.yml temporário criado com sucesso.")

# --- NOVA FUNÇÃO HELPER (O "Pulo do Gato") ---
def download_prod_manifest(github_token):
    print("A iniciar o download do artefacto 'prod-manifest'...")

    # Variáveis de ambiente que o GitHub nos dá
    repo_owner = os.environ['GITHUB_REPOSITORY_OWNER']
    repo_name = os.environ['GITHUB_REPOSITORY'].split('/')[-1]

    # 1. Encontrar o ID do último run bem sucedido na 'main'
    # Usamos o ID do workflow 'generate_manifest.yml'
    list_runs_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/generate_manifest.yml/runs?branch=main&status=success&per_page=1"
    headers = {'Authorization': f'token {github_token}', 'Accept': 'application/vnd.github.v3+json'}

    response = requests.get(list_runs_url, headers=headers)
    if response.status_code != 200 or not response.json()['workflow_runs']:
        raise Exception("Não encontrei nenhum workflow 'generate_manifest.yml' bem sucedido na 'main'.")

    latest_run_id = response.json()['workflow_runs'][0]['id']
    print(f"Encontrei o último run ID da 'main': {latest_run_id}")

    # 2. Listar os artefactos desse run
    list_artifacts_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs/{latest_run_id}/artifacts"
    response = requests.get(list_artifacts_url, headers=headers)
    artifacts = response.json()['artifacts']

    prod_manifest_artifact = next((a for a in artifacts if a['name'] == 'prod-manifest'), None)
    if not prod_manifest_artifact:
        raise Exception("Não encontrei o artefacto 'prod-manifest' no último run da 'main'.")

    download_url = prod_manifest_artifact['archive_download_url']
    print("Encontrei o URL de download do 'prod-manifest'.")

    # 3. Descarregar o artefacto (que é um .zip)
    response = requests.get(download_url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(f"Falha ao descarregar o artefacto: {response.status_code}")

    # 4. Deszipar o artefacto
    # O 'dbt --defer' precisa da pasta 'target'
    # Vamos criar 'prod_state/target' para guardar o manifest.json
    prod_state_dir_parent = os.path.join(os.environ.get('HOME', '/root'), "prod_state")
    prod_state_dir_target = os.path.join(prod_state_dir_parent, "target")
    os.makedirs(prod_state_dir_target, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(prod_state_dir_target) # Extrai o manifest.json para dentro de prod_state/target

    print(f"Artefacto 'prod-manifest' descarregado e deszipado para {prod_state_dir_target}")
    return prod_state_dir_parent # Retorna a pasta PAI ('prod_state')

# --- Função Principal (MODIFICADA) ---
def main():
    print(f"🤖 Action da Joyce [v0.2.0 'Slim CI'] iniciada!")

    conn = None
    cursor = None
    message = ""
    clone_schema = "PR_CLONE_ERROR"

    try:
        # 1. Pegar credenciais e inputs
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']
        with open(event_path) as f:
            event_data = json.load(f)
        comments_url = event_data['pull_request']['comments_url']
        pr_number = event_data['pull_request']['number']

        sf_user = os.environ['INPUT_SF_USER']; sf_password = os.environ['INPUT_SF_PASSWORD']
        sf_account = os.environ['INPUT_SF_ACCOUNT']; sf_warehouse = os.environ['INPUT_SF_WAREHOUSE']
        sf_database = os.environ['INPUT_SF_DATABASE']; sf_role = os.environ['INPUT_SF_ROLE']
        prod_schema = os.environ['INPUT_SF_SCHEMA']
        dbt_dir_relative = os.environ.get('INPUT_DBT_PROJECT_DIR', '.')
        dbt_dir_abs = os.path.join("/github/workspace", dbt_dir_relative)
        clone_schema = f"PR_{pr_number}_CLONE"

        runner_home = os.environ.get('HOME', '/root')
        profiles_dir = os.path.join(runner_home, ".dbt_pr_runner")

        # 2. Conexão (Igual)
        print(f"A conectar ao Snowflake (Conta: {sf_account})...")
        conn = snowflake.connector.connect(
            user=sf_user, password=sf_password, account=sf_account,
            warehouse=sf_warehouse, database=sf_database, role=sf_role
        )
        cursor = conn.cursor()
        print("✅ Conexão com o Snowflake BEM SUCEDIDA!")

        # 3. "Zero-Copy Clone" (Igual)
        print(f"A criar schema 'clone': {clone_schema} a partir de {prod_schema}...")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 4. [NOVO] Descarregar o manifest.json da Produção
        prod_state_dir = download_prod_manifest(token) # Chama a nossa nova função

        # 5. Rodar dbt (COM SLIM CI)
        create_profiles_yml(profiles_dir, sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema)

        run_command(["dbt", "deps"], cwd_dir=dbt_dir_abs, profiles_dir=profiles_dir)

        print("A executar 'dbt build' (Modo SLIM CI)...")
        # ESTE É O COMANDO "FODA" DO SLIM CI

        # Define o caminho correto para o 'state' (onde o manifest.json está)
        state_path = os.path.join(prod_state_dir, "target")

        slim_ci_command = [
            "dbt", "build",
            "--select", "state:modified+",
            "--defer",
            "--state", state_path      # <-- A CORREÇÃO
        ]
        run_command(slim_ci_command, cwd_dir=dbt_dir_abs, profiles_dir=profiles_dir)
        print("✅ 'dbt build' (Slim CI) concluído!")

        # 6. Lógica do "Diff" (Igual à v4.0.2/v0.1.0)
        print("A iniciar o 'diff' (com filtro de unique_id)...")
        message_lines = [
            f"✅ **[TASK 5 & 6]** SUCESSO! (v0.2.0 - SLIM CI)",
            "O `dbt build` (Slim CI) rodou e aqui está o 'diff' de contagem de linhas:", "",
            "| Modelo Modificado | Contagem (Produção) | Contagem (PR) | Mudança |",
            "| :--- | :--- | :--- | :--- |"
        ]

        run_results_path = os.path.join(dbt_dir_abs, "target/run_results.json")
        with open(run_results_path) as f:
            run_results = json.load(f)

        models_built = [r for r in run_results['results'] if r.get('unique_id', '').startswith('model.') and r.get('status') == 'success']

        if not models_built:
            message_lines.append("| *Nenhum modelo (modificado) foi construído.* | | | |")

        for model in models_built:
            model_name = model['unique_id'].split('.')[-1] 
            print(f"A fazer o 'diff' do modelo: {model_name}...")

            cursor.execute(f"SELECT COUNT(*) FROM {sf_database}.{prod_schema}.{model_name}")
            count_prod = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {sf_database}.{clone_schema}.{model_name}")
            count_clone = cursor.fetchone()[0]

            mudanca = count_clone - count_prod
            emoji = "➡️" if mudanca == 0 else ( "⬆️" if mudanca > 0 else "⬇️" )

            message_lines.append(f"| `{model_name}` | {count_prod:,} | {count_clone:,} | {mudanca:+,} {emoji} |")

        message = "\n".join(message_lines)

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = f"❌ **[MVP v2]** FALHA (v0.2.0)\n\n**Erro Recebido:**\n```{e}```"
        post_comment(message)
        sys.exit(1)

    finally:
        # 7. Limpar
        try:
            if cursor:
                print(f"A limpar... a dropar schema {clone_schema}...")
                cursor.execute(f"DROP SCHEMA IF EXISTS {clone_schema};")
                print("Limpeza concluída."); cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            print(f"Erro durante a limpeza: {e}")
            pass 

    # 8. Postar o resultado de SUCESSO no PR
    post_comment(message)

if __name__ == "__main__":
    main()