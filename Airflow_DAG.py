from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.models import Variable
import clickhouse_connect
import os
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'linhtb',
    'depends_on_past': False,
    'start_date': datetime(2023,11,10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'homebase_testing',
    default_args=default_args,
    schedule_interval='0 * * * * ',
    catchup=False)  

    
def postgre_to_clickhouse():
    def pandas_execute_sql_postgre(uri, query):
        try:
            alchemyEngine = create_engine(uri, pool_recycle=3600)
            dbConnection = alchemyEngine.connect()
            dataFrame = pd.read_sql(query, dbConnection)
            dataFrame
            pd.set_option('display.expand_frame_repr', False)
            dbConnection.close()
            return dataFrame
        except Exception as e:
            print(e)
            return None
        finally:
            None
    
    def pandas_write_clickhouse(df,clickhouse_host, clickhouse_port, clickhoust_username, clickhouse_password):
        client = clickhouse_connect.get_client(host=clickhouse_host, port = clickhouse_port, username=clickhoust_username, password=clickhouse_password)
        client.insert('bank_marketing', df)
    
    postgre_uri=Variable.get("POSTGRE_URI", default_var=None)
    postgre_query = "SELECT * FROM bank_marketing WHERE created_at >= DATE_TRUNC('hour', NOW())- INTERVAL '1 hour' AND created_at < DATE_TRUNC('hour', NOW())"

    dropped_columns = ['created_at', 'updated_at']

    df = pandas_execute_sql_postgre(postgre_uri, postgre_query)
    df = df.drop(columns=dropped_columns)
    if df.shape[0] > 1:
        clickhouse_host=Variable.get("CLICKHOUSE_HOST", default_var=None)
        clickhouse_port=Variable.get("CLICKHOUSE_PORT", default_var=None)
        clickhouse_username=Variable.get("CLICKHOUSE_USERNAME", default_var=None)
        clickhouse_password=Variable.get("CLICKHOUSE_PASSWORD", default_var=None)

        pandas_write_clickhouse(df,clickhouse_host, clickhouse_port, clickhouse_username, clickhouse_password)

task=PythonOperator(dag=dag,
                     task_id='homebase_testing',
                     python_callable=postgre_to_clickhouse)
