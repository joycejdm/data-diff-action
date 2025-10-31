# main.py (VERSÃO v2.0.0 - A VERSÃO DA PESQUISA)
import os
import requests
import json
import snowflake.connector
import sys

# --- Função Helper para Postar Comentário (Igual a antes) ---
def post_comment(message_body):
    print(f"A postar comentário: {message_body}")
    try:
        token = os.environ['GITHUB_TOKEN']
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

# --- Função Principal ---
def main():
    print(f"🤖 Action da Joyce [v2.0.0] iniciada!")

    try:
        sf_user = os.environ['INPUT_SF_USER']
        sf_password = os.environ['INPUT_SF_PASSWORD']
        sf_account = os.environ['INPUT_SF_ACCOUNT'] # A conta COMPLETA
        sf_warehouse = os.environ['INPUT_SF_WAREHOUSE']
        sf_database = os.environ['INPUT_SF_DATABASE']
        sf_role = os.environ['INPUT_SF_ROLE']
    except KeyError as e:
        message = f"❌ **[v2.0.0] FALHA CRÍTICA**\n\nNão consegui ler um dos seus `secrets`. O input `{e}` está em falta."
        print(message, file=sys.stderr)
        post_comment(message)
        sys.exit(1)

    try:
        print(f"A tentar conectar com Account='{sf_account}'...")

        # 3. Tentar a conexão (DA FORMA CORRETA E PESQUISADA)
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account, # A conta COMPLETA (ex: wemgvex-rf16823.sa-east-1.aws)
            warehouse=sf_warehouse,
            database=sf_database,
            role=sf_role,
            session_parameters={'QUERY_TAG': 'JoyceSaaS_v2_0_0'}
        )

        print("Conectado! A rodar 'SELECT 1'...")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if result == 1:
            message = "✅ **[TASK 2]** SUCESSO! (v2.0.0)\n\nConexão com o Snowflake BEM SUCEDIDA! (Rodei `SELECT 1`)."
        else:
            raise Exception("Query 'SELECT 1' falhou.")

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = (
            f"❌ **[TASK 2]** FALHA (v2.0.0)\n\n"
            f"**Valor que eu tentei usar:**\n"
            f"* `Account`: `{sf_account}`\n\n"
            f"**Erro Recebido:**\n"
            f"```{e}```"
        )
        post_comment(message)
        sys.exit(1)

    post_comment(message)

if __name__ == "__main__":
    main()