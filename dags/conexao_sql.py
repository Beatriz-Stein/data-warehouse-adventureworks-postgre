from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime
import pandas as pd

def extrair_e_carregar(**context):
    print("Iniciando a extração da Dimensão Produto...")

    # ----- 1. Extrair do SQL Server -----
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')

    sql = """
    SELECT 
        p.ProductKey AS id_produto,
        p.EnglishProductName AS nome,
        p.Color AS cor,
        p.EnglishDescription AS descricao
    FROM dbo.DimProduct AS p
    WHERE p.FinishedGoodsFlag = 1;
    """

    df = mssql_hook.get_pandas_df(sql)
    print(f"Dados extraídos: {len(df)} linhas.")

    # ----- 2. Conectar ao Postgres -----
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()

    # ----- 3. TRUNCATE com CASCADE (limpa antes de gravar) -----
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_produto RESTART IDENTITY CASCADE;"))

    print("Tabela dim_produto limpa.")

    # ----- 4. Carregar nova dimensão -----
    df.to_sql('dim_produto', engine, schema='public', if_exists='append', index=False)

    print("Carga finalizada com sucesso na dim_produto.")


# ===== DAG =====

with DAG(
    'etl_dimensao_produto',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'produto']
) as dag:

    tarefa_mover_dados = PythonOperator(
        task_id='mover_dados',
        python_callable=extrair_e_carregar
    )
