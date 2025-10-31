# main.py
import os
import requests
import json

print("🤖 Action da Joyce iniciada!")

try:
    # 1. Pegar o token que o usuário nos passou
    # O GitHub Actions transforma a entrada 'github-token' em 'INPUT_GITHUB_TOKEN'
    token = os.environ['INPUT_GITHUB_TOKEN']

    # 2. Pegar o caminho do arquivo de "evento" (que tem os dados do PR)
    event_path = os.environ['GITHUB_EVENT_PATH']

    # 3. Ler o arquivo de evento para pegar a URL de comentários do PR
    with open(event_path) as f:
        event_data = json.load(f)

    # 4. Encontrar a URL de comentários (só existe se for um PR)
    if 'pull_request' not in event_data:
        print("Não é um Pull Request. Saindo.")
        exit(0)

    comments_url = event_data['pull_request']['comments_url']

    # 5. Preparar a nossa mensagem
    message_body = "🤖 Olá! Eu sou o bot da Joyce e vi seu PR! Em breve trarei seu 'data diff'... #buildinpublic"

    payload = {
        'body': message_body
    }

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    # 6. Postar o comentário!
    response = requests.post(comments_url, json=payload, headers=headers)

    if response.status_code == 201:
        print("Comentário postado com sucesso!")
    else:
        print(f"Erro ao postar comentário: {response.status_code}")
        print(response.text)
        exit(1) # Falha a Action se não conseguir postar

except Exception as e:
    print(f"Ocorreu um erro: {e}")
    exit(1) # Falha a Action