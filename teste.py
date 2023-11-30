from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os 
import pandas as pd
import psycopg2 as pg



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
    df.to_csv(tableName + '.csv', index=False)
    
if __name__ == "__main__":
    transform(sheetName=1, tableName='derivated_fuels')
    print('Deu certo')

   