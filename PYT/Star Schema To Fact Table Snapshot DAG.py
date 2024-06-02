from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres1"
DB_HOST = "172.17.5.39"
DB_PORT = "5432"
SOURCE_SCHEMA_NAME = "DP_OLAP_HEALTHCARE"
TARGET_SCHEMA_NAME = "DP_FACT_SNAPSHOT_HEALTHCARE"

# Create a database connection engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'olap_fact_table_snapshot',
    default_args=default_args,
    description='Create a Fact Table Snapshot form OLAP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_display_name='Create a Fact Table Snapshot'
)

# Define tasks
# Task 1: Transform Data to create FactTransactions and upload to DB
def create_fact_table_snapshot():
    # Read source tables from OLTP DB into a pandas DataFrame
    FactTransactions = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."FactTransactions"', engine)
    DimPatient = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DimPatient"', engine)
    DimPhysician = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DimPhysician"', engine)
    DimHospital = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DimHospital"', engine)
    DimDiagnosisCode = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DimDiagnosisCode"', engine)
    DimCPTCode = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DimCPTCode"', engine)

    # Rename Dimension Tables to add a Prefix
    DimPatient = DimPatient.rename(columns=lambda x: f'Patient{x}' if x not in ('PatientID') else x)
    DimPhysician = DimPhysician.rename(columns=lambda x: f'Physician{x}' if x not in ('PhysicianID') else x)
    DimHospital = DimHospital.rename(columns=lambda x: f'Hospital{x}' if x not in ('HospitalID', 'HospitalName') else x)

    # Join tables to form the FactTableSnapshot
    FactTableSnapshot = pd.merge(FactTransactions, DimPatient, how='left', on='PatientID')
    FactTableSnapshot = pd.merge(FactTableSnapshot, DimPhysician, how='left', on='PhysicianID')
    FactTableSnapshot = pd.merge(FactTableSnapshot, DimHospital, how='left', on='HospitalID')
    FactTableSnapshot = pd.merge(FactTableSnapshot, DimDiagnosisCode, how='left', on='DiagnosisCodeID')
    FactTableSnapshot = pd.merge(FactTableSnapshot, DimCPTCode, how='left', on='CPTCodeID')

    # Write the new data to PostgreSQL table
    FactTableSnapshot.to_sql("FactTableSnapshot", engine, schema=TARGET_SCHEMA_NAME, if_exists="replace", index=False)
    print(f"FactTableSnapshot has been uploaded to {TARGET_SCHEMA_NAME}")

# Define tasks for each table
task_create_fact_table_snapshot = PythonOperator(
    task_id='create_fact_table_snapshot',
    python_callable=create_fact_table_snapshot,
    provide_context=True,
    task_display_name='Task 1: Transform and Upload FactTableSnapshot Table',
    dag=dag,
)

# Define task dependencies to establish the order of execution
task_create_fact_table_snapshot