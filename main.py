# main.py (VERSÃO v2.2.2 - Faz o Build e o Diff de Contagem)
import os
import requests
import json
import snowflake.connector
import subprocess
import sys

# --- Função Helper para Postar Comentário (Igual a antes) ---
def post_comment(message_body):
    print(f"A postar comentário...")
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

# --- Função Helper para Rodar Comandos (Igual a antes) ---
def run_command(command, cwd_dir):
    print(f"Executando: {' '.join(command)} (no diretório: {cwd_dir})")
    runner_home = os.environ.get('HOME', '/root')
    profiles_dir = os.path.join(runner_home, ".dbt")
    command.extend(["--profiles-dir", profiles_dir])
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', cwd=cwd_dir)
    if result.returncode != 0:
        print("--- ERRO no Subprocess ---"); print(result.stdout); print(result.stderr)
        raise Exception(f"Comando falhou: {' '.join(command)}. Veja o log da Action para detalhes.")
    print("--- Saída do Subprocess ---"); print(result.stdout)
    return result.stdout

# --- Função Helper para Criar o profiles.yml (Igual a antes) ---
def create_profiles_yml(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema):
    print("A criar profiles.yml temporário...")
    runner_home = os.environ.get('HOME', '/root')
    dbt_profile_path = os.path.join(runner_home, ".dbt")
    os.makedirs(dbt_profile_path, exist_ok=True)
    profiles_yml_content = f"""
    default:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: {sf_account}
          user: {sf_user}
          password: {sf_password}
          role: {sf_role}
          warehouse: {sf_warehouse}
          database: {sf_database}
          schema: {clone_schema}
          threads: 1
    """
    with open(os.path.join(dbt_profile_path, "profiles.yml"), "w") as f:
        f.write(profiles_yml_content)
    print("profiles.yml temporário criado com sucesso.")


# --- Função Principal (AQUI ESTÃO AS MUDANÇAS) ---
def main():
    print(f"🤖 Action da Joyce [v2.2.2] iniciada!")

    conn = None
    cursor = None
    message = ""
    clone_schema = "PR_CLONE_ERROR"

    try:
        # 1. Pegar credenciais e inputs (Igual)
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

        # 2. [TASK 2] Conexão (Igual)
        print(f"A conectar ao Snowflake (Conta: {sf_account})...")
        conn = snowflake.connector.connect(
            user=sf_user, password=sf_password, account=sf_account,
            warehouse=sf_warehouse, database=sf_database, role=sf_role
        )
        cursor = conn.cursor()
        print("✅ [TASK 2] Conexão com o Snowflake BEM SUCEDIDA!")

        # 3. [TASK 3] Lógica de "Zero-Copy Clone" (Igual)
        print(f"A criar schema 'clone': {clone_schema} a partir de {prod_schema}...")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 4. [TASK 4] Rodar dbt (*** MUDANÇA AQUI ***)
        create_profiles_yml(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema)
        run_command(["dbt", "deps"], cwd_dir=dbt_dir_abs)

        print("A executar 'dbt build'...")
        # MUDANÇA: Trocamos 'parse' por 'build' para *executar* os modelos
        # Estamos a usar '--select state:modified+' para construir SÓ o que mudou
        # Para isso funcionar, precisamos de um 'defer' para o estado de produção
        # MVP MAIS SIMPLES: Vamos só rodar 'dbt build' no projeto inteiro.
        # O clone já tem o estado da produção, o 'build' vai recriar
        # os modelos modificados (fct_vendas) e pular os não modificados.
        run_command(["dbt", "build"], cwd_dir=dbt_dir_abs)
        print("✅ 'dbt build' concluído!")

        # 5. [TASK 5] Lógica do "Diff" (MODO DE DIAGNÓSTICO v2.2.3)
        print("A iniciar o 'diff' (Modo de Diagnóstico)...")

        run_results_path = os.path.join(dbt_dir_abs, "target/run_results.json")
        with open(run_results_path) as f:
            run_results = json.load(f)

        # Vamos ver o que está dentro de 'results'
        results_diagnostico = []
        for r in run_results['results']:
            status = r.get('status')
            resource_type = r.get('resource_type')
            unique_id = r.get('unique_id')
            results_diagnostico.append(f"- Status: `{status}`, Tipo: `{resource_type}`, ID: `{unique_id}`")

        message_lines = [
            "✅ **[DIAGNÓSTICO]** SUCESSO! (v2.2.3)",
            "O `dbt build` rodou. Aqui está o que eu encontrei no `run_results.json`:",
            "",
            "\n".join(results_diagnostico),
            "",
            "Próximo passo: Usar isto para corrigir o filtro!"
        ]

        message = "\n".join(message_lines)

        # O resto do 'except', 'finally' e 'post_comment' pode ficar igual
        # Apenas certifique-se de que a mensagem de ERRO também diz 'v2.2.3'

    except Exception as e:
        # 6. Reportar Erro (Igual)
        print(f"ERRO: {e}", file=sys.stderr)
        message = f"❌ **[TASK 5,6]** FALHA (v2.2.2)\n\n**Erro Recebido:**\n```{e}```"
        post_comment(message)
        sys.exit(1)

    finally:
        # 7. Limpar (Igual)
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

    # 8. [TASK 6] Postar o resultado de SUCESSO no PR
    post_comment(message)

if __name__ == "__main__":
    main()