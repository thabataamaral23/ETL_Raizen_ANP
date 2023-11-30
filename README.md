# ETL_Raizen_ANP
ETL process to extract data from ANP report, transform and persist in a PostgreSQL database.

Raw Data:
- vendas_combustiveis-m3.xlsx

Files after transformation:
- diesel.csv
- derivated_fuels.csv

Code files:
- **ETL_raizen.py** DAG Code.
- **documentation.pdf** describes the solution and some initial analysis from the modeled database.

Setup
- Airflow: localhost - Porta 8029.
- PostgreSQL
