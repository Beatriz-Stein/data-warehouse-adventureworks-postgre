from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime
import pandas as pd


def etl_dim_cliente(**context):
    print("Iniciando extração da Dimensão Cliente...")

    # Conexão com SQL Server (fonte)
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')

    sql = """
    SELECT 
        c.CustomerKey AS id_cliente,
        c.FirstName AS primeiro_nome,
        c.LastName AS ultimo_nome,
        c.Gender AS genero,
        c.EmailAddress AS email,
        g.City AS cidade,
        g.EnglishCountryRegionName AS pais
    FROM dbo.DimCustomer c
    LEFT JOIN dbo.DimGeography g 
           ON c.GeographyKey = g.GeographyKey;
    """

    df = mssql_hook.get_pandas_df(sql)
    print(f"Linhas extraídas: {len(df)}")

    # Conexão com Postgres (destino)
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()

    # Limpa a tabela para recarga completa
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_cliente RESTART IDENTITY CASCADE;"))

    print("Tabela dim_cliente limpa.")

    # Carga dos dados
    df.to_sql('dim_cliente', engine, schema='public', if_exists='append', index=False)

    print("Carga da dimensão cliente concluída.")


with DAG(
    dag_id='etl_dimensao_cliente',
    description='Carga da dimensão cliente do SQL Server para o PostgreSQL (DW)',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'cliente']
) as dag:

    mover_cliente = PythonOperator(
        task_id='mover_dados_cliente',
        python_callable=etl_dim_cliente
    )
