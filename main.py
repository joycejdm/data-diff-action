# main.py (VERSÃO DE DIAGNÓSTICO v1.0.2)
import os
import requests
import json

print("🤖 SCRIPT DE DIAGNÓSTICO v1.0.2 ESTÁ A RODAR!")

try:
    token = os.environ['INPUT_GITHUB_TOKEN']
    event_path = os.environ['GITHUB_EVENT_PATH']

    with open(event_path) as f:
        event_data = json.load(f)

    comments_url = event_data['pull_request']['comments_url']

    # A nova mensagem de teste
    message = "🤖 **TESTE v1.0.2 BEM SUCEDIDO!**\n\nSe você está a ver isto, o cache foi limpo e o `main.py` foi atualizado. Agora podemos voltar a tentar a conexão com o Snowflake."

    payload = {'body': message}
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    response = requests.post(comments_url, json=payload, headers=headers)

    if response.status_code == 201:
        print("Comentário de diagnóstico postado!")
    else:
        print(f"Erro ao postar diagnóstico: {response.text}")
        exit(1)

except Exception as e:
    print(f"Erro no script de diagnóstico: {e}")
    exit(1)