import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

def write_data():
    df = pd.read_csv("product_results.csv", delimiter='~')
    conn = BaseHook.get_connection('postgres_ch')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    df.to_sql(name='new_table', con=engine, if_exists='append', index=False)