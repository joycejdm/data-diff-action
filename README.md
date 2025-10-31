# Data Diff Action (MVP v0.1.0)

Esta é uma GitHub Action (criada em #buildinpublic) que atua como um "inspetor de segurança" para os seus Pull Requests do dbt.

Ela impede que "pesadelos silenciosos" (mudanças de lógica que degradam os dados) cheguem à produção, fazendo um "diff" dos seus dados *antes* do merge.

## O que este MVP (v0.1.0) faz?

Esta é a primeira versão funcional. O seu *loop* de CI/CD é o seguinte:

1.  **Acionado por PR:** A Action é acionada sempre que um PR é aberto para a branch `main`.
2.  **Conexão Segura:** Conecta-se ao Snowflake usando os *secrets* do seu repositório.
3.  **Zero-Copy Clone:** Cria um "schema fantasma" (`PR_X_CLONE`) usando o `ZERO-COPY CLONE` do Snowflake (instantâneo e barato) a partir do seu schema de produção.
4.  **Execução do dbt:** Cria um `profiles.yml` temporário e roda `dbt build` (ou `dbt run`) dentro desse schema "fantasma".
5.  **Diff (Contagem de Linhas):** O bot compara o `COUNT(*)` de todos os modelos que foram construídos (no *schema clone*) com os seus equivalentes na *produção*.
6.  **Report no PR:** O bot posta um comentário de volta no PR com uma tabela de resultados, mostrando qualquer mudança na contagem de linhas.

---

## Como Usar

### 1. Crie os "Secrets" no GitHub

No seu repositório (o que tem o projeto dbt), vá para `Settings` > `Secrets and variables` > `Actions` e adicione os seguintes "Repository secrets":

| Secret | Descrição (Exemplo de Valor) |
| :--- | :--- |
| `SF_ACCOUNT` | A sua conta completa do Snowflake |
| `SF_USER` | O seu utilizador do dbt  |
| `SF_PASSWORD` | A sua senha |
| `SF_ROLE` | O role que o dbt vai usar |
| `SF_WAREHOUSE` | O warehouse que o dbt vai usar  |
| `SF_DATABASE` | O banco de dados do seu projeto  |
| `SF_SCHEMA` | O seu schema de **produção** |

### 2. Crie o Ficheiro de Workflow

Na raiz do seu projeto dbt, crie o ficheiro `.github/workflows/data_diff.yml`:

```yaml
# .github/workflows/data_diff.yml

name: 'CI - Data Diff'

on:
  pull_request:
    branches: [ main ] # Ou a sua branch de produção

permissions:
  pull-requests: write # Necessário para o bot postar comentários

jobs:
  run-data-diff:
    runs-on: ubuntu-latest
    steps:
      # 1. Faz o checkout do seu código (para o dbt poder ler os modelos)
      - name: Checkout código
        uses: actions/checkout@v4

      # 2. Roda a "Data Diff Action"
      - name: Rodar Joyce Data Diff Action
        # Mude 'joycejdm' pelo seu usuário do GitHub!
        uses: joycejdm/data-diff-action@v0.1.0 
        
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          sf_account: ${{ secrets.SF_ACCOUNT }}
          sf_user: ${{ secrets.SF_USER }}
          sf_password: ${{ secrets.SF_PASSWORD }}
          sf_role: ${{ secrets.SF_ROLE }}
          sf_warehouse: ${{ secrets.SF_WAREHOUSE }}
          sf_database: ${{ secrets.SF_DATABASE }}
          sf_schema: ${{ secrets.SF_SCHEMA }}
          
          # Opcional: Se o seu dbt_project.yml não está na raiz
          # dbt_project_dir: 'pasta/do/dbt'
```


## Roadmap (Próximos Passos)

[ ] Diff Estatístico: Adicionar SUM(), AVG(), COUNT(DISTINCT) e % de NULLs.

[ ] Slim CI: Implementar state:modified+ para rodar apenas o que mudou.

[ ] Integrar data-diff: Usar o CLI open-source data-diff para performance.