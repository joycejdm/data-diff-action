# main.py (VERSÃO v2.1.0 - Conecta, Clona e Roda dbt)
import os
import requests
import json
import snowflake.connector
import subprocess # Para rodar comandos (dbt)
import sys

# --- Função Helper para Postar Comentário (Igual a antes) ---
def post_comment(message_body):
    print(f"A postar comentário: {message_body}")
    try:
        token = os.environ['INPUT_GITHUB_TOKEN']
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

# --- Função Helper para Rodar Comandos (NOVO) ---
def run_command(command, cwd_dir):
    print(f"Executando: {' '.join(command)} (no diretório: {cwd_dir})")

    # Pega o $HOME do runner para o profiles.yml
    runner_home = os.environ.get('HOME', '/root') # /root é o default no container
    profiles_dir = os.path.join(runner_home, ".dbt")

    # Adiciona o profiles_dir ao comando
    command.extend(["--profiles-dir", profiles_dir])

    # O 'cwd' diz ao subprocess para rodar o comando *dentro* da pasta do dbt
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', cwd=cwd_dir)

    if result.returncode != 0:
        print("--- ERRO no Subprocess ---")
        print(result.stdout)
        print(result.stderr)
        raise Exception(f"Comando falhou: {' '.join(command)}. Veja o log da Action para detalhes.")
    print("--- Saída do Subprocess ---")
    print(result.stdout)
    return result.stdout

# --- Função Helper para Criar o profiles.yml (NOVO) ---
def create_profiles_yml(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema):
    print("A criar profiles.yml temporário...")

    # Pega o $HOME do runner
    runner_home = os.environ.get('HOME', '/root')
    dbt_profile_path = os.path.join(runner_home, ".dbt")
    os.makedirs(dbt_profile_path, exist_ok=True)

    # O dbt-core precisa de um profiles.yml.
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
          schema: {clone_schema} # IMPORTANTE: O dbt vai escrever no schema CLONE
          threads: 1
    """

    with open(os.path.join(dbt_profile_path, "profiles.yml"), "w") as f:
        f.write(profiles_yml_content)

    print("profiles.yml temporário criado com sucesso.")


# --- Função Principal ---
def main():
    print(f"🤖 Action da Joyce [v2.1.0] iniciada!")

    conn = None
    cursor = None
    message = ""
    clone_schema = "PR_CLONE_ERROR" # Default

    try:
        # 1. Pegar credenciais e inputs
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']

        with open(event_path) as f:
            event_data = json.load(f)

        comments_url = event_data['pull_request']['comments_url']
        pr_number = event_data['pull_request']['number']

        sf_user = os.environ['INPUT_SF_USER']
        sf_password = os.environ['INPUT_SF_PASSWORD']
        sf_account = os.environ['INPUT_SF_ACCOUNT']
        sf_warehouse = os.environ['INPUT_SF_WAREHOUSE']
        sf_database = os.environ['INPUT_SF_DATABASE']
        sf_role = os.environ['INPUT_SF_ROLE']
        prod_schema = os.environ['INPUT_SF_SCHEMA']

        # O diretório do dbt (onde o 'dbt_project.yml' está)
        # /github/workspace é onde a Action faz o checkout do código do "cobaia"
        dbt_dir = os.path.join("/github/workspace", os.environ.get('INPUT_DBT_PROJECT_DIR', '.'))

        # O nome do nosso schema "fantasma"
        clone_schema = f"PR_{pr_number}_CLONE"

        print(f"A conectar ao Snowflake (Conta: {sf_account})...")

        # 2. [TASK 2] Conexão (Já sabemos que funciona!)
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            role=sf_role
        )
        cursor = conn.cursor()
        print("✅ [TASK 2] Conexão com o Snowflake BEM SUCEDIDA!")

        # 3. [TASK 3] Lógica de "Zero-Copy Clone"
        print(f"A criar schema 'clone': {clone_schema} a partir de {prod_schema}...")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 4. [TASK 4] Rodar dbt
        # Criar o profiles.yml que o dbt vai usar
        create_profiles_yml(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema)

        # Rodar `dbt deps` (necessário)
        run_command(["dbt", "deps"], cwd_dir=dbt_dir)

        # Rodar `dbt parse` (prova que o dbt consegue ler o projeto)
        run_command(["dbt", "parse"], cwd_dir=dbt_dir)

        # Rodar `dbt build` (vamos rodar só o que mudou - mais avançado, por agora 'parse' chega)

        message = (
            f"✅ **[TASK 2, 3, 4]** SUCESSO! (v2.1.0)\n\n"
            f"1. Conexão com Snowflake: **OK**\n"
            f"2. Criação do Schema Clone (`{clone_schema}`): **OK**\n"
            f"3. `dbt deps` e `dbt parse` no diretório '{dbt_dir}': **OK**\n\n"
            f"O seu bot agora sabe rodar `dbt`! Próxima task!"
        )

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = (
            f"❌ **[TASK 2,3,4]** FALHA (v2.1.0)\n\n"
            f"**Erro Recebido:**\n"
            f"```{e}```"
        )
        post_comment(message)
        sys.exit(1)

    finally:
        # 5. Limpar (Sempre tentar dropar o schema clone)
        try:
            if cursor:
                print(f"A limpar... a dropar schema {clone_schema}...")
                cursor.execute(f"DROP SCHEMA IF EXISTS {clone_schema};")
                print("Limpeza concluída.")
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            print(f"Erro durante a limpeza: {e}")
            pass 

    # 6. Postar o resultado de SUCESSO no PR
    post_comment(message)

if __name__ == "__main__":
    main()