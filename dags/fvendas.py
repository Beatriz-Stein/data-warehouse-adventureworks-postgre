from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime
import pandas as pd

def etl_fato_vendas(**context):
    print("Iniciando extração da Fato Vendas...")

    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')

    sql = """
    SELECT 
        SalesOrderNumber AS num_pedido,
        CustomerKey AS id_cliente,
        ProductKey AS id_produto,
        OrderDate AS data_pedido,
        DueDate AS data_vencimento,
        ShipDate AS data_envio,
        OrderQuantity AS quantidade,
        UnitPrice AS preco_unitario,
        SalesAmount AS valor_total
    FROM dbo.FactInternetSales;
    """

    df = mssql_hook.get_pandas_df(sql)
    print(f"Linhas extraídas: {len(df)}")

    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.fato_vendas RESTART IDENTITY CASCADE;"))

    df.to_sql("fato_vendas", engine, schema="public", if_exists="append", index=False)

    print("Carga concluída: fato_vendas.")


with DAG(
    'etl_fato_vendas',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'fato']
) as dag:

    mover_fato_vendas = PythonOperator(
        task_id='mover_fato_vendas',
        python_callable=etl_fato_vendas
    )
