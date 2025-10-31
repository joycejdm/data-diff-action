# main.py (VERSÃO v1.1.0 - Conecta, Clona e Roda dbt parse)
import os
import requests
import json
import snowflake.connector
import subprocess # Para rodar comandos (dbt)

# --- Função Helper para Postar Comentário ---
def post_comment(token, comments_url, message_body):
    payload = {'body': message_body}
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    response = requests.post(comments_url, json=payload, headers=headers)
    if response.status_code == 201:
        print("Comentário postado com sucesso!")
    else:
        print(f"Erro ao postar comentário: {response.status_code}")
        print(response.text)
        exit(1)

# --- Função Helper para Rodar Comandos ---
def run_command(command):
    print(f"Executando: {' '.join(command)}")
    # Usamos /github/workspace como o diretório de trabalho padrão
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', cwd='/github/workspace')
    if result.returncode != 0:
        print("--- ERRO no Subprocess ---")
        print(result.stdout)
        print(result.stderr)
        raise Exception(f"Comando falhou: {' '.join(command)}. Veja o log da Action para detalhes.")
    print("--- Saída do Subprocess ---")
    print(result.stdout)
    return result.stdout

# --- Função Principal ---
def main():
    print(f"🤖 Action da Joyce [TASK 2,3,4 - v1.1.0] iniciada!")

    # Estas variáveis são definidas fora do 'try' para o 'finally' conseguir aceder
    conn = None
    cursor = None
    comments_url = None
    token = None
    clone_schema = "PR_CLONE_ERROR" # Default
    message = "" # Default

    try:
        # 1. Pegar credenciais do GitHub
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']

        with open(event_path) as f:
            event_data = json.load(f)

        comments_url = event_data['pull_request']['comments_url']
        pr_number = event_data['pull_request']['number']

        # 2. Pegar as credenciais do Snowflake
        sf_user = os.environ['INPUT_SF_USER']
        sf_password = os.environ['INPUT_SF_PASSWORD']
        sf_account = os.environ['INPUT_SF_ACCOUNT']
        sf_region = os.environ['INPUT_SF_REGION']
        sf_warehouse = os.environ['INPUT_SF_WAREHOUSE']
        sf_database = os.environ['INPUT_SF_DATABASE']
        sf_role = os.environ['INPUT_SF_ROLE']
        prod_schema = os.environ['INPUT_SF_SCHEMA'] # Schema de Produção
        dbt_dir = os.environ['INPUT_DBT_PROJECT_DIR']

        # O nome do nosso schema "fantasma"
        clone_schema = f"PR_{pr_number}_CLONE"

        print(f"A tentar conectar com Account='{sf_account}' e Region='{sf_region}'...")

        # 3. [TASK 2] Tentar a conexão (DA FORMA CORRETA)
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account, # Passado separadamente
            region=sf_region,     # Passado separadamente
            warehouse=sf_warehouse,
            database=sf_database,
            role=sf_role
        )
        cursor = conn.cursor()
        print("✅ Conexão com o Snowflake BEM SUCEDIDA!")

        # 4. [TASK 3] Lógica de "Zero-Copy Clone"
        print(f"A criar schema 'clone': {clone_schema} a partir de {prod_schema}...")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 5. [TASK 4] Rodar dbt (Simplificado por agora)
        print("A preparar para rodar dbt...")

        # O dbt-core precisa de um profiles.yml. Vamos criar um temporário.
        profiles_yml_content = f"""
        default:
          target: dev
          outputs:
            dev:
              type: snowflake
              account: {sf_account}
              region: {sf_region}
              user: {sf_user}
              password: {sf_password}
              role: {sf_role}
              warehouse: {sf_warehouse}
              database: {sf_database}
              schema: {clone_schema} # IMPORTANTE: O dbt vai escrever no schema CLONE
              threads: 1
        """

        runner_home = os.environ['HOME']
        dbt_profile_path = os.path.join(runner_home, ".dbt")
        os.makedirs(dbt_profile_path, exist_ok=True)
        with open(os.path.join(dbt_profile_path, "profiles.yml"), "w") as f:
            f.write(profiles_yml_content)

        print("profiles.yml temporário criado.")

        # Rodar `dbt deps` (necessário)
        run_command(["dbt", "deps", "--project-dir", dbt_dir, "--profiles-dir", dbt_profile_path])

        # Rodar `dbt parse` (prova que o dbt consegue ler o projeto)
        run_command(["dbt", "parse", "--project-dir", dbt_dir, "--profiles-dir", dbt_profile_path])

        message = (
            f"✅ **[TASK 2, 3, 4]** SUCESSO!\n\n"
            f"1. Conexão com Snowflake: **OK**\n"
            f"2. Criação do Schema Clone (`{clone_schema}`): **OK**\n"
            f"3. `dbt parse` (prova que o dbt leu o projeto): **OK**\n\n"
            f"Próxima task é o 'diff'!"
        )

    except Exception as e:
        # 6. Se falhar, reportar o erro (com o nosso debug)
        print(f"ERRO: {e}")
        message = (
            f"❌ **[TASK 2,3,4]** FALHA.\n\n"
            f"**Valores que eu tentei usar:**\n"
            f"* `Account`: `{os.environ.get('INPUT_SF_ACCOUNT', 'N/A')}`\n"
            f"* `Region`: `{os.environ.get('INPUT_SF_REGION', 'N/A')}`\n\n"
            f"**Erro Recebido:**\n"
            f"```{e}```"
        )
    finally:
        # 7. Limpar (Sempre tentar dropar o schema clone)
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
            pass # Falha silenciosamente se a limpeza falhar

        # 8. Postar o resultado no PR (só se tivermos o URL)
        if token and comments_url:
            post_comment(token, comments_url, message)
        else:
            print("Não foi possível postar comentário (erro antes de pegar token/url).")

# --- Rodar o script ---
if __name__ == "__main__":
    main()