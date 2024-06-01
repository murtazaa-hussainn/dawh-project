from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres1"
DB_HOST = "172.17.5.39"
DB_PORT = "5432"
SCHEMA_NAME = "DP_OLTP_HEALTHCARE"

# Create a database connection engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Get the last processed date from Airflow variables
last_processed_date = Variable.get('OLTP_LAST_PROCESSED_DATE', default_var='2022-01-01')
last_processed_date = datetime.strptime(last_processed_date, '%Y-%m-%d')
processing_range = Variable.get('PROCESSING_RANGE', default_var=1)
max_processed_date = last_processed_date + relativedelta(months=int(processing_range))

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
    'upload_csv_to_postgres_incremental',
    default_args=default_args,
    description='Upload CSV files to PostgreSQL incrementally',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_display_name='Upload CSV to PostgreSQL OLTP'
)

# Define tasks
# Task 1: Upload Billing to DB
def upload_billing_to_postgres():
    # Read data from CSV file into a pandas DataFrame
    df = pd.read_csv(f"/home/murtaza/Spaghetti/Billing.csv")

    # Convert column to datetime
    df['BillDate'] = pd.to_datetime(df['BillDate'])

    # Filter the DataFrame based on the last processed date
    new_data = df[(df['BillDate'] > last_processed_date) & (df['BillDate'] <= max_processed_date)]

    # Write the new data to PostgreSQL table
    if not new_data.empty:
        new_data.to_sql("Billing", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"Billing has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for Billing")

# Task 2: Upload Patient to DB
def upload_patient_to_postgres():
    # Read data from CSV file into a pandas DataFrame
    df = pd.read_csv(f"/home/murtaza/Spaghetti/Patient.csv")

    # Convert column to datetime
    df['RegistrationDate'] = pd.to_datetime(df['RegistrationDate'])

    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Filter the DataFrame based on the max processed date
        new_data = df[df['RegistrationDate'] <= max_processed_date]

    else:
        # Filter the DataFrame based on the last processed date and max processed date
        new_data = df[(df['RegistrationDate'] > last_processed_date) & (df['RegistrationDate'] <= max_processed_date)]

    # Write the new data to PostgreSQL table
    if not new_data.empty:
        new_data.to_sql("Patient", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"Patient has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for Patient")

# Task 3: Upload PatientSurvey to DB
def upload_patient_survey_to_postgres():
    # Read data from CSV file into a pandas DataFrame
    df = pd.read_csv(f"/home/murtaza/Spaghetti/PatientSurvey.csv")

    # Convert column to datetime
    df['SurveyDate'] = pd.to_datetime(df['SurveyDate'])

    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Filter the DataFrame based on the max processed date
        new_data = df[df['SurveyDate'] <= max_processed_date]

    else:
        # Filter the DataFrame based on the last processed date and max processed date
        new_data = df[(df['SurveyDate'] > last_processed_date) & (df['SurveyDate'] <= max_processed_date)]

    # Write the new data to PostgreSQL table
    if not new_data.empty:
        new_data.to_sql("PatientSurvey", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"PatientSurvey has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for PatientSurvey")

# Task 4: Upload Provider to DB
def upload_provider_to_postgres():
    # Check if Data is already present in the Table
    query = f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."Provider"'
    existing_count = engine.execute(query).scalar()

    if existing_count == 0:
        # Read data from CSV file into a pandas DataFrame
        df = pd.read_csv(f"/home/murtaza/Spaghetti/Provider.csv")

        # Write the new data to PostgreSQL table
        df.to_sql("Provider", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"Provider has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for Provider")

# Task 5: Upload ProviderSpecialty to DB
def upload_provider_specialty_to_postgres():
    # Check if Data is already present in the Table
    query = f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."ProviderSpecialty"'
    existing_count = engine.execute(query).scalar()

    if existing_count == 0:
        # Read data from CSV file into a pandas DataFrame
        df = pd.read_csv(f"/home/murtaza/Spaghetti/ProviderSpecialty.csv")

        # Write the new data to PostgreSQL table
        df.to_sql("ProviderSpecialty", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"ProviderSpecialty has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for ProviderSpecialty")

# Task 6: Upload ClinicLocations to DB
def upload_clinic_locations_to_postgres():
    # Check if Data is already present in the Table
    query = f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."ClinicLocations"'
    existing_count = engine.execute(query).scalar()

    if existing_count == 0:
        # Read data from CSV file into a pandas DataFrame
        df = pd.read_csv(f"/home/murtaza/Spaghetti/ClinicLocations.csv")

        # Write the new data to PostgreSQL table
        df.to_sql("ClinicLocations", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"ClinicLocations has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for ClinicLocations")

# Task 7: Upload ClinicalReports to DB
def upload_clinical_reports_to_postgres():
    # Read data from CSV file into a pandas DataFrame
    df = pd.read_csv(f"/home/murtaza/Spaghetti/ClinicalReports.csv")

    # Convert column to datetime
    df['GeneratedDate'] = pd.to_datetime(df['GeneratedDate'])

    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Filter the DataFrame based on the max processed date
        new_data = df[df['GeneratedDate'] <= max_processed_date]

    else:
        # Filter the DataFrame based on the last processed date and max processed date
        new_data = df[(df['GeneratedDate'] > last_processed_date) & (df['GeneratedDate'] <= max_processed_date)]

    # Write the new data to PostgreSQL table
    if not new_data.empty:
        new_data.to_sql("ClinicalReports", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"ClinicalReports has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for ClinicalReports")

# Task 8: Upload CptCodesLookup to DB
def upload_cpt_codes_lookup_to_postgres():
    # Check if Data is already present in the Table
    query = f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."CptCodesLookup"'
    existing_count = engine.execute(query).scalar()

    if existing_count == 0:
        # Read data from CSV file into a pandas DataFrame
        df = pd.read_csv(f"/home/murtaza/Spaghetti/CptCodesLookup.csv")

        # Write the new data to PostgreSQL table
        df.to_sql("CptCodesLookup", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"CptCodesLookup has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for CptCodesLookup")

# Task 9: Upload DiagnosisCodeLookup to DB
def upload_diagnosis_code_lookup_to_postgres():
    # Check if Data is already present in the Table
    query = f'SELECT COUNT(*) FROM "{SCHEMA_NAME}"."DiagnosisCodeLookup"'
    existing_count = engine.execute(query).scalar()

    if existing_count == 0:
        # Read data from CSV file into a pandas DataFrame
        df = pd.read_csv(f"/home/murtaza/Spaghetti/DiagnosisCodeLookup.csv")

        # Write the new data to PostgreSQL table
        df.to_sql("DiagnosisCodeLookup", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        print(f"DiagnosisCodeLookup has been uploaded to {SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DiagnosisCodeLookup")

# Task 10: Update Last Processed Date Variable
def update_last_processed_date():
    # Update the last processed date
    Variable.set('OLTP_LAST_PROCESSED_DATE', max_processed_date.strftime('%Y-%m-%d'))


# Define tasks for each table
task_upload_billing = PythonOperator(
    task_id='upload_billing_to_postgres',
    python_callable=upload_billing_to_postgres,
    provide_context=True,
    task_display_name='Task 1: Upload Billing Table to OLTP',
    dag=dag,
)

task_upload_patient = PythonOperator(
    task_id='upload_patient_to_postgres',
    python_callable=upload_patient_to_postgres,
    provide_context=True,
    task_display_name='Task 2: Upload Patient Table to OLTP',
    dag=dag,
)

task_upload_patient_survey = PythonOperator(
    task_id='upload_patient_survey_to_postgres',
    python_callable=upload_patient_survey_to_postgres,
    provide_context=True,
    task_display_name='Task 3: Upload PatientSurvey Table to OLTP',
    dag=dag,
)

task_upload_provider = PythonOperator(
    task_id='upload_provider_to_postgres',
    python_callable=upload_provider_to_postgres,
    provide_context=True,
    task_display_name='Task 4: Upload Provider Table to OLTP',
    dag=dag,
)

task_upload_provider_specialty = PythonOperator(
    task_id='upload_provider_specialty_to_postgres',
    python_callable=upload_provider_specialty_to_postgres,
    provide_context=True,
    task_display_name='Task 5: Upload ProviderSpecialty Table to OLTP',
    dag=dag,
)

task_upload_clinic_locations = PythonOperator(
    task_id='upload_clinic_locations_to_postgres',
    python_callable=upload_clinic_locations_to_postgres,
    provide_context=True,
    task_display_name='Task 6: Upload ClinicLocations Table to OLTP',
    dag=dag,
)

task_upload_clinical_reports = PythonOperator(
    task_id='upload_clinical_reports_to_postgres',
    python_callable=upload_clinical_reports_to_postgres,
    provide_context=True,
    task_display_name='Task 7: Upload ClinicalReports Table to OLTP',
    dag=dag,
)

task_upload_cpt_codes_lookup = PythonOperator(
    task_id='upload_cpt_codes_lookup_to_postgres',
    python_callable=upload_cpt_codes_lookup_to_postgres,
    provide_context=True,
    task_display_name='Task 8: Upload CptCodesLookup Table to OLTP',
    dag=dag,
)

task_upload_diagnosis_code_lookup = PythonOperator(
    task_id='upload_diagnosis_code_lookup_to_postgres',
    python_callable=upload_diagnosis_code_lookup_to_postgres,
    provide_context=True,
    task_display_name='Task 9: Upload DiagnosisCodeLookup Table to OLTP',
    dag=dag,
)

task_update_last_processed_date = PythonOperator(
    task_id='update_last_processed_date',
    python_callable=update_last_processed_date,
    provide_context=True,
    task_display_name='Task 10: Update LAST_PROCESSED_DATE Variable in Airflow',
    dag=dag,
)

# Define task dependencies to establish the order of execution
task_upload_billing >> task_upload_patient >> task_upload_patient_survey >> task_upload_provider >> \
task_upload_provider_specialty >> task_upload_clinic_locations >> task_upload_clinical_reports >> \
task_upload_cpt_codes_lookup >> task_upload_diagnosis_code_lookup >> task_update_last_processed_date