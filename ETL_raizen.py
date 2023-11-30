from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os
import pandas as pd
import psycopg2 as pg

# Initializing the default arguments
default_args = {
    'owner': 'Thabata',
    'start_date': datetime(2023, 11, 29)
}

# Define the DAG for Airflow
dag = DAG('etl_raizen_anp', description='ETL process to extract data from ANP report',
          schedule_interval="59 23 L * *",
          default_args=default_args,
          tags=["etl_anp", "data"],
          max_active_runs=1,
          catchup=False)

# Get the current timestamp for creating records in the database
current_timestamp = datetime.now()

def etl_transform(sheet_name, table_name):
    """
    Transform the data from the Excel file and save it as a CSV.

    Parameters:
    - sheet_name (int): The sheet index in the Excel file.
    - table_name (str): The name of the table.
    """
    
    # Read the original data from the Excel file
    df = pd.read_excel('../raw_data/vendas-combustiveis-m3.xlsx', sheet_name=sheet_name)

    # Perform data transformation
    df.columns = ['Combustível', 'Ano', 'Região', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Total']
        
    # Reshape the DataFrame using the melt function
    df = pd.melt(df, id_vars=['Combustível', 'Ano', 'Região', 'UF'], var_name='variable', value_name='volume')
        
    # Filter out rows with 'Total' in the 'variable' column
    df = df[df['variable'] != 'Total']
        
    # Create 'year_month' column by combining 'Ano' and 'variable' columns
    df['year_month'] = pd.to_datetime(df['Ano'].astype(str) + '-' + df['variable'])
        
    # Drop unnecessary columns
    df = df.drop(labels=['variable', 'Região', 'Ano'], axis=1)
        
    # Rename columns for consistency
    df.columns = ['product', 'uf', 'volume', 'year_month']
        
    # Convert 'volume' to numeric
    df['volume'] = pd.to_numeric(df['volume'])
        
    # Fill NaN values with 0
    df = df.fillna(0)
        
    # Add 'unit' column
    df['unit'] = 'm3'
        
    # Add 'created_at' column with the current timestamp
    df['created_at'] = datetime.now()   

    # Create staging directory if not exists
    directory_path = '../staging'
    os.makedirs(directory_path, exist_ok=True)

    # Save the transformed data as CSV
    df.to_csv(os.path.join(directory_path, f'{table_name}.csv'), index=False)

def etl_load(table_name):
    """
    Load the transformed data into a PostgreSQL database.

    Parameters:
    - table_name (str): The name of the table.

    """
    try:
        # Connect to the default database
        with pg.connect(database='postgres', user='postgres', password='postgres', host='localhost', port=5432) as db_connect:
            # Create the table if it does not already exist
            with db_connect.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        year_month date,
                        uf VARCHAR(256),
                        product VARCHAR(256),
                        unit VARCHAR(256),
                        volume FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT {table_name}_pk PRIMARY KEY (year_month, uf, product, unit) 
                    );
                    
                    TRUNCATE TABLE {table_name}
                """)
                print(f'Table {table_name} created')

            # Insert each CSV row as a record in the database
            with open(f"{table_name}.csv") as f:
                next(f)  # Skip the first row (header)
                print(f)
                with db_connect.cursor() as cursor:
                    for row in f:
                        cursor.execute("""
                            INSERT INTO {} (year_month, uf, product, unit, volume, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (year_month, uf, product, unit)
                            DO UPDATE 
                            SET
                                volume = EXCLUDED.volume,
                                created_at = EXCLUDED.created_at;
                        """.format(table_name), (
                            row.split(",")[3],
                            row.split(",")[1],
                            row.split(",")[0],
                            row.split(",")[4],
                            row.split(",")[2],
                            current_timestamp
                        ))
                db_connect.commit()

    except Exception as error:
        print(f"Error: {error}")

# Define the PythonOperators for Airflow tasks
extract_derivated_fuels = PythonOperator(
    task_id = 'extract_derivated_fuels', 
    python_callable = etl_transform,
    op_kwargs = {'sheet_name': 1, 'table_name': 'derivated_fuels'},
    dag = dag
)

extract_diesel = PythonOperator(
    task_id = 'extract_diesel', 
    python_callable = etl_transform,
    op_kwargs = {'sheet_name': 2, 'table_name': 'diesel'},
    dag = dag
)

load_diesel_db = PythonOperator(
    task_id = "load_diesel_db",
    python_callable = etl_load,
    op_kwargs = {'table_name': 'diesel'},
    dag = dag
)

load_derivated_fuel_db = PythonOperator(
    task_id = "load_derivated_fuel_db",
    python_callable = etl_load,
    op_kwargs = {'table_name': 'derivated_fuels'},
    dag = dag
)

# Set task dependencies
extract_diesel >> load_diesel_db
extract_derivated_fuels >> load_derivated_fuel_db
