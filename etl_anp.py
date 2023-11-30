from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os
import pandas as pd
import psycopg2 as pg
from datetime import datetime

# initializing the default arguments
default_args = {
    'owner': 'Thabata',
    'start_date': datetime(2023, 11, 29),
    'retries': 3
    # 'retry_delay': timedelta(minutes=5)
}

current_timestamp = datetime.now()

def transform(sheetName, tableName):
    # Read the original data from the Excel file
    df = pd.read_excel('../raw_data/vendas-combustiveis-m3.xlsx', sheet_name=sheetName)
    df.columns = ['Combustível', 'Ano', 'Região', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Total']
    df = df.melt(id_vars=['Combustível', 'Ano', 'Região', 'UF'])
    df = df.loc[df['variable'] != 'Total']
    df['year_month'] = df['Ano'].astype(str) + '-' + df['variable']
    df['year_month'] = pd.to_datetime(df['year_month'])
    df = df.drop(labels=['variable', 'Região', 'Ano'], axis=1)
    df.columns = ['product', 'uf', 'volume', 'year_month']
    df['volume'] = pd.to_numeric(df['volume'])
    df = df.fillna(0)
    df['unit'] = 'm3'
    df['created_at'] = datetime.now()

    directory_path = '../staging'
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory created.")
    else:
        print(f"Directory already exists.")
    df.to_csv(directory_path + '/' + tableName + '.csv', index=False)


dag = DAG('etl-raizen-final_100', description='ETL process to extract internal pivot caches from consolidated reports ANP',
          start_date=datetime(2023, 11, 29), catchup=False)


def sql_load(table_name):
    dbconnect = None

    try:

        # Connect to the 'raizen_anp' database
        dbconnect = pg.connect(
            database='postgres',
            user='postgres',
            password='postgres',
            host='localhost',
            port=5432
        )

        # Create the table if it does not already exist
        with dbconnect.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id serial PRIMARY KEY,
                    year_month date,
                    uf VARCHAR(256),
                    product VARCHAR(256),
                    unit VARCHAR(256),
                    volume FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT {table_name}_unique_constraint_name_product_uf UNIQUE (year_month, uf, product, unit) 
                )
                PARTITION BY RANGE (EXTRACT(YEAR FROM year_month))
                ;
                TRUNCATE TABLE {table_name}""")
            dbconnect.commit()
            print(f'Table {table_name} created')

        # Insert each CSV row as a record in the database
        with open(f"{table_name}.csv") as f:
            next(f)  # Skip the first row (header)
            print(f)
            with dbconnect.cursor() as cursor:
                for row in f:
                    # Use parameterized query to avoid SQL injection

                    cursor.execute(f"""
                        INSERT INTO {table_name} (year_month, uf, product, unit, volume, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (year_month, uf, product, unit)  -- Use the appropriate unique constraint for your scenario
                        DO UPDATE 
                        SET
                            volume = EXCLUDED.volume,
                            created_at = EXCLUDED.created_at;
                    """, (
                        row.split(",")[3],
                        row.split(",")[1],
                        row.split(",")[0],
                        row.split(",")[4],
                        row.split(",")[2],
                        current_timestamp
                    ))
                    dbconnect.commit()



    except Exception as error:
        print(f"Error: {error}")
    finally:
        if dbconnect:
            dbconnect.close()

extract_derivated_fuels  = PythonOperator(
        task_id='extract_derivated_fuels', 
        python_callable=transform,
        op_kwargs={'sheetName': 1, 'tableName': 'derivated_fuels'},
        dag=dag)


extract_diesel  = PythonOperator(
        task_id='extract_diesel', 
        python_callable=transform,
        op_kwargs={'sheetName': 2, 'tableName': 'diesel'},
        dag=dag)


loadDieselToSql = PythonOperator(
        task_id="loadDieselToSql",
        python_callable=sql_load,
        op_kwargs={'table_name': 'diesel'}
    )

loadDerivatedFuelToSQL = PythonOperator(
        task_id="loadDerivatedFuelToSql",
        python_callable=sql_load,
        op_kwargs={'table_name': 'derivated_fuels'}
    )
        
extract_diesel >> loadDieselToSql
extract_derivated_fuels >> loadDerivatedFuelToSQL
