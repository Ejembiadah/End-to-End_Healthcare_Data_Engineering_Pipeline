# import Libraries
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
#import kagglehub

#path = kagglehub.dataset_download("prasad22/healthcare-dataset")
#print('path to dataset:', path)

# File path
file_location = r"C:\Users\DELL\.cache\kagglehub\datasets\prasad22\healthcare-dataset\versions\2\healthcare_dataset.csv"

# PostgreSQL connection details
db_user = 'your_username'
db_pass = 'your_password'
db_host = 'localhost'  
db_port = '5432'
db_name = 'your_database'

# Extract Data
def extract_health_dataset(**kwargs):
    df = pd.read_csv(file_location)
    kwargs['ti'].xcom_push(key = 'Health_dataset', value=df)

# Transform Data
def transform_health_dataset(**kwargs):
    df = kwargs['ti'].xcom_pull(key = 'Health_dataset')
    # Convert columns to datetime
    columns = ['Date of Admission', 'Discharge Date']
    for cols in columns:
        df[cols] = pd.to_datetime(df[cols])

    # Round(2) 'Billing Amount'
    df['Billing Amount'] = round(df['Billing Amount'], 2)

    # Convert name to proper case
    df['Name'] = df['Name'].str.title()  

    # Add column 'Length of Stay (in Days)' = Discharge Date - Date of Admission
    df['Length of Stay (in Days)'] = (df['Discharge Date'] - df['Date of Admission']).dt.days

    kwargs['ti'].xcom_push(key="health_df_transformed", value=df)

# Load data to postgresql
def load_health_dataset(**kwargs):
    df = kwargs["ti"].xcom_pull(key="health_df_transformed")
    

    # Create connection
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    # Load data
    df.to_sql('Health_data', engine, if_exists = 'replace', index=False)

# Airflow DAG
with DAG(
    dag_id = 'Health_etl',
    start_date = datetime(2024, 1, 1),
    schedule_interval = None,
    catch_up = False,
    tags = ['etl', 'postgres', 'health']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_health_data',
        python_callable=extract_health_dataset
    )
     
    transform_task = PythonOperator(
        task_id = 'transform_health_data',
        python_callable=transform_health_dataset
    )

    load_task = PythonOperator(
        task_id = 'load_health_data',
        python_callable=load_health_dataset
    )

extract_task >> transform_task >> load_task
#if __name__ == "__main__":
    #df = extract_health_dataset()
    #if df is not None:
        #df = transform_health_dataset(df)
        #print(df.head())
        #load_health_dataset(df)

