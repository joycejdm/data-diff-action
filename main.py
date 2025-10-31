# main.py
import os
import requests
import json
import snowflake.connector # Importamos a nova biblioteca!

print("🤖 Action da Joyce iniciada! [TASK 2: Conectar ao Snowflake]")

def post_comment(token, comments_url, body):
    """Função para postar um comentário no PR"""
    payload = {'body': body}
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

def test_snowflake_connection():
    """Função para testar a conexão com o Snowflake"""
    print("Iniciando teste de conexão com o Snowflake...")
    try:
        # 1. Ler as credenciais do Snowflake (passadas como inputs)
        user = os.environ['INPUT_SNOWFLAKE_USER']
        password = os.environ['INPUT_SNOWFLAKE_PASSWORD']
        account = os.environ['INPUT_SNOWFLAKE_ACCOUNT']
        region = os.environ['INPUT_SNOWFLAKE_REGION']
        database = os.environ['INPUT_SNOWFLAKE_DATABASE']
        schema = os.environ['INPUT_SNOWFLAKE_SCHEMA']

        # O formato da conta para o conector é 'account.region'
        # (Ex: 'wemgvex-rf16823.sa-east-1')
        full_account = f"{account}.{region}"

        # 2. Conectar!
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=full_account,
            database=database,
            schema=schema
        )

        # 3. Rodar o 'SELECT 1'
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        if result[0] == 1:
            print("✅ Conexão com o Snowflake BEM SUCEDIDA!")
            return "✅ Conexão com o Snowflake BEM SUCEDIDA!"
        else:
            raise Exception("Falha ao rodar SELECT 1")

    except Exception as e:
        print(f"❌ ERRO ao conectar no Snowflake: {e}")
        # Retorna a mensagem de erro para ser postada no PR
        return f"❌ ERRO ao conectar no Snowflake: {e}"

# --- LÓGICA PRINCIPAL ---
try:
    # 1. Pegar dados do PR (igual a antes)
    token = os.environ['INPUT_GITHUB_TOKEN']
    event_path = os.environ['GITHUB_EVENT_PATH']

    with open(event_path) as f:
        event_data = json.load(f)

    if 'pull_request' not in event_data:
        print("Não é um Pull Request. Saindo.")
        exit(0)

    comments_url = event_data['pull_request']['comments_url']

    # 2. Postar o comentário "Olá" (igual a antes)
    post_comment(token, comments_url, "🤖 Olá! Estou a conectar no Snowflake agora...")

    # 3. TESTAR O SNOWFLAKE (A parte nova!)
    connection_message = test_snowflake_connection()

    # 4. Postar o resultado da conexão
    post_comment(token, comments_url, connection_message)

except Exception as e:
    print(f"Ocorreu um erro geral: {e}")
    # Tenta postar o erro geral no PR se possível
    try:
        post_comment(os.environ['INPUT_GITHUB_TOKEN'], os.environ['GITHUB_EVENT_PATH'], f"❌ Ocorreu um erro geral na Action: {e}")
    except:
        pass # Se falhar, só falha
    exit(1)