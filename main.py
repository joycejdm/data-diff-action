# main.py (VERSÃO v0.5.0 - Motor data-diff)
import os
import requests
import json
import snowflake.connector
import subprocess
import sys
import zipfile
import io

# --- Função Helper para Postar Comentário (Igual) ---
def post_comment(message_body):
    print(f"A postar comentário...")
    try:
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']
        with open(event_path) as f: event_data = json.load(f)
        comments_url = event_data['pull_request']['comments_url']
        payload = {'body': message_body}
        headers = { 'Authorization': f'token {token}', 'Accept': 'application/vnd.github.v3+json' }
        response = requests.post(comments_url, json=payload, headers=headers)
        if response.status_code != 201:
            print(f"Erro ao postar comentário: {response.text}")
    except Exception as e:
        print(f"Falha crítica ao tentar postar comentário: {e}")

# --- Função Helper para Rodar Comandos (Igual) ---
def run_command(command, cwd_dir, profiles_dir):
    print(f"Executando: {' '.join(command)} (no diretório: {cwd_dir})")
    command.extend(["--profiles-dir", profiles_dir])
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', cwd=cwd_dir)
    if result.returncode != 0:
        print("--- ERRO no Subprocess ---"); print(result.stdout); print(result.stderr)
        raise Exception(f"Comando falhou: {' '.join(command)}. Veja o log da Action para detalhes.")
    print("--- Saída do Subprocess ---"); print(result.stdout)
    return result.stdout

# --- Função Helper para Criar o profiles.yml (Igual) ---
def create_profiles_yml(profiles_dir, sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema):
    print("A criar profiles.yml temporário...")
    os.makedirs(profiles_dir, exist_ok=True)
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
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        f.write(profiles_yml_content)
    print("profiles.yml temporário criado com sucesso.")

# --- Função Helper para Descarregar o Manifest (Igual) ---
def download_prod_manifest(github_token):
    print("A iniciar o download do artefacto 'prod-manifest'...")
    repo_owner = os.environ['GITHUB_REPOSITORY_OWNER']; repo_name = os.environ['GITHUB_REPOSITORY'].split('/')[-1]
    list_runs_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/generate_manifest.yml/runs?branch=main&status=success&per_page=1"
    headers = {'Authorization': f'token {github_token}', 'Accept': 'application/vnd.github.v3+json'}
    response = requests.get(list_runs_url, headers=headers)
    if response.status_code != 200 or not response.json()['workflow_runs']:
        raise Exception("Não encontrei nenhum workflow 'generate_manifest.yml' bem sucedido na 'main'.")
    latest_run_id = response.json()['workflow_runs'][0]['id']
    list_artifacts_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs/{latest_run_id}/artifacts"
    response = requests.get(list_artifacts_url, headers=headers)
    artifacts = response.json()['artifacts']
    prod_manifest_artifact = next((a for a in artifacts if a['name'] == 'prod-manifest'), None)
    if not prod_manifest_artifact:
        raise Exception("Não encontrei o artefacto 'prod-manifest' no último run da 'main'.")
    download_url = prod_manifest_artifact['archive_download_url']
    response = requests.get(download_url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(f"Falha ao descarregar o artefacto: {response.status_code}")
    prod_state_dir_parent = os.path.join(os.environ.get('HOME', '/root'), "prod_state")
    prod_state_dir_target = os.path.join(prod_state_dir_parent, "target")
    os.makedirs(prod_state_dir_target, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(prod_state_dir_target)
    print(f"Artefacto 'prod-manifest' descarregado e deszipado para {prod_state_dir_target}")
    return prod_state_dir_parent

# --- Função Helper (Schema Diff) (Igual) ---
def get_schema_info(cursor, database, schema):
    print(f"A ler schema: {database}.{schema}")
    sql_get_cols = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE 
    FROM {database}.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema.upper()}'
    """
    cursor.execute(sql_get_cols)
    schema_info = {}
    for row in cursor.fetchall():
        table_name, col_name, col_type = row[0], row[1], row[2]
        if table_name not in schema_info:
            schema_info[table_name] = {}
        schema_info[table_name][col_name] = col_type
    return schema_info

# --- Função Principal (MODIFICADA) ---
def main():
    print(f"🤖 Action da Joyce [v0.5.0 'data-diff Engine'] iniciada!")

    conn = None; cursor = None; message = ""; clone_schema = "PR_CLONE_ERROR"

    try:
        # 1. Pegar credenciais (Igual)
        token = os.environ['INPUT_GITHUB_TOKEN']
        event_path = os.environ['GITHUB_EVENT_PATH']
        with open(event_path) as f: event_data = json.load(f)
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
        conn = snowflake.connector.connect(
            user=sf_user, password=sf_password, account=sf_account,
            warehouse=sf_warehouse, database=sf_database, role=sf_role
        )
        cursor = conn.cursor()
        print("✅ Conexão com o Snowflake BEM SUCEDIDA!")

        # 3. "Zero-Copy Clone" (Igual)
        cursor.execute(f"CREATE OR REPLACE TRANSIENT SCHEMA {clone_schema} CLONE {prod_schema};")
        print(f"Schema {clone_schema} criado com sucesso.")

        # 4. Descarregar o manifest.json da Produção (Igual)
        prod_state_dir = download_prod_manifest(token)

        # 5. Rodar dbt (Igual - Slim CI)
        create_profiles_yml(profiles_dir, sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, clone_schema)
        run_command(["dbt", "deps"], cwd_dir=dbt_dir_abs, profiles_dir=profiles_dir)
        print("A executar 'dbt build' (Modo SLIM CI)...")
        state_path = os.path.join(prod_state_dir, "target")
        slim_ci_command = ["dbt", "build", "--select", "state:modified+", "--defer", "--state", state_path]
        run_command(slim_ci_command, cwd_dir=dbt_dir_abs, profiles_dir=profiles_dir)
        print("✅ 'dbt build' (Slim CI) concluído!")

        # 6. [TASK 5] Lógica do "Diff" (*** O "UPGRADE FODA" ***)
        print("A iniciar o 'Relatório de Impacto'...")

        # Cabeçalho da Mensagem
        message_lines = [f"✅ **[MVP v2] SUCESSO! (v0.5.0 - Motor data-diff)**\n",
                         "O `dbt build` (Slim CI) rodou. Aqui está o seu Relatório de Impacto:\n"]

        # --- LÓGICA DO SCHEMA DIFF (Mantida) ---
        schema_diff_report = ["---", "🛡️ **Relatório de Schema**\n"]
        prod_schema_info = get_schema_info(cursor, sf_database, prod_schema)
        clone_schema_info = get_schema_info(cursor, sf_database, clone_schema)
        all_models = set(prod_schema_info.keys()) | set(clone_schema_info.keys())
        schema_changes_found = False
        for model_name in all_models:
            prod_cols = prod_schema_info.get(model_name, {})
            clone_cols = clone_schema_info.get(model_name, {})
            for col_name in prod_cols:
                if col_name in clone_cols and prod_cols[col_name] != clone_cols[col_name]:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ⚠️ **Tipo Alterado** | `{prod_cols[col_name]}` -> `{clone_cols[col_name]}` |")
            for col_name in prod_cols:
                if col_name not in clone_cols:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ❌ **DROPADA** | |")
            for col_name in clone_cols:
                if col_name not in prod_cols:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ✅ **ADICIONADA** | |")
        if not schema_changes_found:
            schema_diff_report.append("*Nenhuma mudança de schema detetada.*")
        else:
            schema_diff_report.insert(1, "| Modelo | Coluna | Mudança | Detalhes |"); schema_diff_report.insert(2, "| :--- | :--- | :--- | :--- |")
        message_lines.extend(schema_diff_report)

        # --- LÓGICA DO DATA DIFF (*** NOVA ***) ---
        stats_diff_report = ["\n---\n", "📊 **Relatório de Dados (Motor `data-diff`)**\n",
                             "| Modelo | Status | Diferença |",
                             "| :--- | :--- | :--- |"]

        run_results_path = os.path.join(dbt_dir_abs, "target/run_results.json")
        with open(run_results_path) as f: run_results = json.load(f)
        models_built = [r for r in run_results['results'] if r.get('unique_id', '').startswith('model.') and r.get('status') == 'success']

        if not models_built:
            stats_diff_report.append("| *Nenhum modelo (modificado) foi construído.* | | |")

        # Define as senhas como variáveis de ambiente para o `data-diff`
        os.environ['DATA_DIFF_PASSWORD'] = sf_password
        os.environ['DATA_DIFF_PASSWORD2'] = sf_password

        for model in models_built:
            model_name_upper = model['unique_id'].split('.')[-1].upper()
            print(f"A rodar 'data-diff' no modelo: {model_name_upper}...")

            # Define as tabelas de produção e clone
            tabela_prod = f"{sf_database}.{prod_schema}.{model_name_upper}"
            tabela_clone = f"{sf_database}.{clone_schema}.{model_name_upper}"

            # Encontrar a Chave Primária (o 'data-diff' precisa disto)
            # Vamos assumir que a primeira coluna é a chave (isto é uma simplificação de MVP)
            # O ideal seria ler o 'schema.yml' do dbt, mas isso é v0.6.0!
            primary_key = prod_schema_info[model_name_upper].keys()
            if not primary_key:
                raise Exception(f"Modelo {model_name_upper} não tem colunas no schema_info.")
            primary_key = list(primary_key)[0] # Pega a primeira coluna
            print(f"A usar a chave primária (suposta): {primary_key}")

            # Constrói o comando 'data-diff' (sem senhas)
            diff_command = [
                "data-diff",
                "--driver", "snowflake",
                "--host", sf_account,
                "--user", sf_user,
                "--warehouse", sf_warehouse,
                "--database", sf_database,
                tabela_prod, # Tabela 1
                tabela_clone, # Tabela 2
                "--json",
                "--key-columns", primary_key
            ]

            # Roda o comando 'data-diff' (sem a nossa helper 'run_command')
            result = subprocess.run(diff_command, capture_output=True, text=True, encoding='utf-8')

            if result.returncode == 0:
                # Sucesso, mas o que ele disse?
                diff_json = json.loads(result.stdout)
                is_diff = diff_json['diff_percent'] > 0
                if is_diff:
                    diff_report = f"❌ **Diferente** ({diff_json['diff_percent']:.2f}%)"
                else:
                    diff_report = "✅ **Idêntico**"
                stats_diff_report.append(f"| `{model_name_upper}` | `{diff_report}` | {diff_json['total']} linhas |")
            else:
                # O 'data-diff' falhou
                print(f"--- ERRO no data-diff ---"); print(result.stdout); print(result.stderr)
                stats_diff_report.append(f"| `{model_name_upper}` | ⚠️ **Erro** | `Falha ao rodar data-diff` |")


        message_lines.extend(stats_diff_report)
        message = "\n".join(message_lines)

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = f"❌ **[MVP v2]** FALHA (v0.5.0)\n\n**Erro Recebido:**\n```{e}```"
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

    # 8. Postar o resultado de SUCESSO no PR
    post_comment(message)

if __name__ == "__main__":
    main()