# main.py (VERSÃO v4.0.0 - A "FORMA CORRETA" FINAL)
import os
import requests
import json
import snowflake.connector
import subprocess
import sys

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
    # ESTA É A LÓGICA DE CONEXÃO DA SUA PESQUISA (v2.0.0)
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

# --- Função Principal ---
def main():
    print(f"🤖 Action da Joyce [v4.0.0] iniciada!")

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
        profiles_dir = os.path.join(runner_home, ".dbt_pr_runner") # Um path único para o profiles

        # 2. Conexão (LÓGICA DA SUA PESQUISA v2.0.0)
        print(f"A conectar ao Snowflake (Conta: {sf_account})...")
        conn = snowflake.connector.connect(
            user=sf_user, password=sf_password, account=sf_account,
            warehouse=sf_warehouse, database=sf_database, role=sf_role
        )
        cursor = conn.cursor()
        print("✅ Conexão com o Snowflake BEM SUCEDIDA!")

        # 3. "Zero-Copy Clone"
        print(f"A criar schema 'clone': {clone_schema} a partir de {prod_schema}...")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 4. Rodar dbt
        create_profiles_yml(profiles_dir, sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema)

        # Usar 'dbt build' que sabemos que funciona (do v2.2.3)
        print("A executar 'dbt build'...")
        run_command(["dbt", "build"], cwd_dir=dbt_dir_abs, profiles_dir=profiles_dir)
        print("✅ 'dbt build' concluído!")

        # 5. [TASK 5] Lógica do "Diff" (MODO DE DIAGNÓSTICO FINAL v4.0.1)
        print("A iniciar o 'diagnóstico final'...")

        run_results_path = os.path.join(dbt_dir_abs, "target/run_results.json")

        try:
            with open(run_results_path) as f:
                # Não vamos fazer 'json.load()'. Vamos ler como texto puro.
                raw_results_content = f.read()
        except Exception as e:
            raise Exception(f"Não consegui LER o ficheiro 'run_results.json'. Erro: {e}")

        message_lines = [
            "✅ **[DIAGNÓSTICO FINAL]** (v4.0.1)",
            "O `dbt build` rodou. Aqui está o conteúdo *cru* do `target/run_results.json`:",
            "",
            "```json",
            raw_results_content[:3000], # Limita aos primeiros 3000 caracteres
            "```",
            "",
            "Agora podemos *ver* o que está lá dentro e corrigir o filtro."
        ]

        message = "\n".join(message_lines)

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = f"❌ **[TASK 5,6]** FALHA (v4.0.0)\n\n**Erro Recebido:**\n```{e}```"
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