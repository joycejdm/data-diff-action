# main.py (VERSÃO v0.4.0 - SCHEMA + STATS DIFF)
import os
import requests
import json
import snowflake.connector
import subprocess
import sys
import zipfile
import io
from decimal import Decimal

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

# --- NOVA Função Helper (Formatação) ---
def format_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, Decimal) or isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}"

# --- NOVA Função Helper (O "Cérebro" do Schema Diff) ---
def get_schema_info(cursor, database, schema):
    """
    Query INFORMATION_SCHEMA e retorna um dicionário
    Ex: {'FCT_VENDAS': {'COLUNA_A': 'TYPE_A', 'COLUNA_B': 'TYPE_B'}}
    """
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
    print(f"🤖 Action da Joyce [v0.4.0 'Schema Diff'] iniciada!")

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
        message_lines = [f"✅ **[MVP v2] SUCESSO! (v0.4.0 - Schema + Stats)**\n",
                         "O `dbt build` (Slim CI) rodou. Aqui está o seu Relatório de Impacto:\n"]

        # --- NOVO: LÓGICA DO SCHEMA DIFF ---
        schema_diff_report = ["---", "🛡️ **Relatório de Schema**\n"]
        prod_schema_info = get_schema_info(cursor, sf_database, prod_schema)
        clone_schema_info = get_schema_info(cursor, sf_database, clone_schema)

        all_models = set(prod_schema_info.keys()) | set(clone_schema_info.keys())
        schema_changes_found = False

        for model_name in all_models:
            prod_cols = prod_schema_info.get(model_name, {})
            clone_cols = clone_schema_info.get(model_name, {})

            # Colunas alteradas (tipo de dado)
            for col_name in prod_cols:
                if col_name in clone_cols and prod_cols[col_name] != clone_cols[col_name]:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ⚠️ **Tipo Alterado** | `{prod_cols[col_name]}` -> `{clone_cols[col_name]}` |")

            # Colunas dropadas
            for col_name in prod_cols:
                if col_name not in clone_cols:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ❌ **DROPADA** | |")

            # Colunas adicionadas
            for col_name in clone_cols:
                if col_name not in prod_cols:
                    schema_changes_found = True
                    schema_diff_report.append(f"| `{model_name}` | `{col_name}` | ✅ **ADICIONADA** | |")

        if not schema_changes_found:
            schema_diff_report.append("*Nenhuma mudança de schema detetada.*")
        else:
            # Adiciona o cabeçalho da tabela se houver mudanças
            schema_diff_report.insert(1, "| Modelo | Coluna | Mudança | Detalhes |")
            schema_diff_report.insert(2, "| :--- | :--- | :--- | :--- |")

        message_lines.extend(schema_diff_report)

        # --- LÓGICA DO STATS DIFF (ATUALIZADA) ---
        stats_diff_report = ["\n---\n", "📊 **Relatório Estatístico (em modelos construídos)**\n",
                             "| Modelo | Métrica | Produção | PR | Mudança |",
                             "| :--- | :--- | :--- | :--- | :--- |"]

        run_results_path = os.path.join(dbt_dir_abs, "target/run_results.json")
        with open(run_results_path) as f: run_results = json.load(f)

        models_built = [r for r in run_results['results'] if r.get('unique_id', '').startswith('model.') and r.get('status') == 'success']

        if not models_built:
            stats_diff_report.append("| *Nenhum modelo (modificado) foi construído.* | | | | |")

        for model in models_built:
            model_name_upper = model['unique_id'].split('.')[-1].upper()
            print(f"A fazer o 'diff' estatístico do modelo: {model_name_upper}...")

            # Apanha as colunas numéricas COMUNS (para não falhar se uma for dropada)
            prod_cols = prod_schema_info.get(model_name_upper, {})
            clone_cols = clone_schema_info.get(model_name_upper, {})

            numeric_types = ('NUMBER', 'FLOAT', 'DECIMAL', 'INT', 'INTEGER', 'DOUBLE')
            common_numeric_cols = [
                col for col, type in prod_cols.items() 
                if col in clone_cols and type == clone_cols[col] and type in numeric_types
            ]
            print(f"Colunas numéricas comuns encontradas: {common_numeric_cols}")

            # --- Construir a query de estatísticas ---
            aggs = ["COUNT(*)"]
            for col in common_numeric_cols:
                aggs.append(f"SUM({col})"); aggs.append(f"AVG({col})")

            sql_agg_string = ", ".join(aggs)

            # --- Executar as queries de estatísticas ---
            cursor.execute(f"SELECT {sql_agg_string} FROM {sf_database}.{prod_schema}.{model_name_upper}")
            prod_stats = cursor.fetchone()
            cursor.execute(f"SELECT {sql_agg_string} FROM {sf_database}.{clone_schema}.{model_name_upper}")
            clone_stats = cursor.fetchone()

            # --- Construir a tabela de resultados ---
            stat_index = 0

            # COUNT(*)
            count_prod, count_clone = prod_stats[stat_index], clone_stats[stat_index]
            mudanca = (count_clone or 0) - (count_prod or 0)
            emoji = "➡️" if mudanca == 0 else ( "⬆️" if mudanca > 0 else "⬇️" )
            stats_diff_report.append(f"| `{model_name_upper}` | `COUNT(*)` | {format_value(count_prod)} | {format_value(count_clone)} | {format_value(mudanca)} {emoji} |")
            stat_index += 1

            # Outras estatísticas (SUM, AVG)
            for col in common_numeric_cols:
                # SUM
                sum_prod, sum_clone = prod_stats[stat_index], clone_stats[stat_index]
                mudanca = (sum_clone or 0) - (sum_prod or 0)
                emoji = "➡️" if mudanca == 0 else ( "⬆️" if mudanca > 0 else "⬇️" )
                stats_diff_report.append(f"| | `SUM({col})` | {format_value(sum_prod)} | {format_value(sum_clone)} | {format_value(mudanca)} {emoji} |")
                stat_index += 1

                # AVG
                avg_prod, avg_clone = prod_stats[stat_index], clone_stats[stat_index]
                mudanca = (avg_clone or 0) - (avg_prod or 0)
                emoji = "➡️" if mudanca == 0 else ( "⬆️" if mudanca > 0 else "⬇️" )
                stats_diff_report.append(f"| | `AVG({col})` | {format_value(avg_prod)} | {format_value(avg_clone)} | {format_value(mudanca)} {emoji} |")
                stat_index += 1

        message_lines.extend(stats_diff_report)
        message = "\n".join(message_lines)

    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        message = f"❌ **[MVP v2]** FALHA (v0.4.0)\n\n**Erro Recebido:**\n```{e}```"
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