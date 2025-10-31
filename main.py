# main.py (VERSÃO CORRETA v1.0.3)
import os
import requests
import json
import snowflake.connector # Importamos o conector!

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

# --- Função Principal ---
def run_connection_test():
    print("🤖 Action da Joyce [TASK 2 - v1.0.3] iniciada!")

    try:
        # 1. Pegar credenciais do GitHub
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']

        with open(event_path) as f:
            event_data = json.load(f)

        comments_url = event_data['pull_request']['comments_url']

        # 2. Pegar as credenciais do Snowflake
        sf_user = os.environ['INPUT_SF_USER']
        sf_password = os.environ['INPUT_SF_PASSWORD']
        sf_account = os.environ['INPUT_SF_ACCOUNT']
        sf_region = os.environ['INPUT_SF_REGION']
        sf_warehouse = os.environ['INPUT_SF_WAREHOUSE']
        sf_database = os.environ['INPUT_SF_DATABASE']
        sf_role = os.environ['INPUT_SF_ROLE']

        print(f"A tentar conectar com Account='{sf_account}' e Region='{sf_region}'...")

        # 3. Tentar a conexão (DA FORMA CORRETA)
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account, # Passado separadamente
            region=sf_region,     # Passado separadamente
            warehouse=sf_warehouse,
            database=sf_database,
            role=sf_role
        )

        # 4. Se conectar, rodar um teste
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()[0]

        if result == 1:
            print("Conexão bem sucedida!")
            message = "✅ **[TASK 2]** Conexão com o Snowflake BEM SUCEDIDA! (Rodei `SELECT 1`). Próxima task!"
        else:
            raise Exception("Query 'SELECT 1' falhou.")

    except Exception as e:
        # 5. Se falhar, reportar o erro (com o nosso debug)
        print(f"ERRO: {e}")
        message = (
            f"❌ **[TASK 2]** FALHA ao conectar no Snowflake.\n\n"
            f"**Valores que eu tentei usar:**\n"
            f"* `Account`: `{os.environ.get('INPUT_SF_ACCOUNT', 'N/A')}`\n"
            f"* `Region`: `{os.environ.get('INPUT_SF_REGION', 'N/A')}`\n\n"
            f"**Erro Recebido:**\n"
            f"```{e}```"
        )

    # 6. Postar o resultado no PR
    post_comment(token, comments_url, message)

# --- Rodar o script ---
if __name__ == "__main__":
    run_connection_test()