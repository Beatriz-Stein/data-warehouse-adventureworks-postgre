from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd

def etl_dim_tempo(**context):
    print("Gerando Dimensão Tempo...")

    start = datetime(2010, 1, 1)
    end = datetime(2030, 12, 31)

    dates = pd.date_range(start, end, freq='D')
    df = pd.DataFrame({
        'data': dates,
        'ano': dates.year,
        'mes': dates.month,
        'dia': dates.day,
        'nome_mes': dates.strftime('%B'),
        'dia_semana': dates.weekday + 1,
        'nome_dia_semana': dates.strftime('%A')
    })

    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_tempo RESTART IDENTITY CASCADE;"))

    df.to_sql('dim_tempo', engine, schema='public', if_exists='append', index=False)

    print("Carga concluída: dim_tempo.")


with DAG(
    'etl_dimensao_tempo',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'tempo']
) as dag:

    gerar_dim_tempo = PythonOperator(
        task_id='gerar_dimensao_tempo',
        python_callable=etl_dim_tempo
    )
