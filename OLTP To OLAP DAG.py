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
SOURCE_SCHEMA_NAME = "DP_OLTP_HEALTHCARE"
TARGET_SCHEMA_NAME = "DP_OLAP_HEALTHCARE"

# Create a database connection engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Get the last processed date from Airflow variables
last_processed_date = Variable.get('OLAP_LAST_PROCESSED_DATE', default_var='2022-01-01')
last_processed_date = datetime.strptime(last_processed_date, '%Y-%m-%d')
max_processed_date = Variable.get('OLTP_LAST_PROCESSED_DATE', default_var='2022-01-01')
max_processed_date = datetime.strptime(max_processed_date, '%Y-%m-%d')

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
    'transform_oltp_to_olap',
    default_args=default_args,
    description='Transform Spaghetti Schema To Star Schema',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_display_name='Transform Spaghetti Schema To Star Schema'
)

# Define tasks
# Task 1: Transform Data to create FactTransactions and upload to DB
def load_transform_fact_transactions():
    # Check if there is new data in OLTP or not
    if max_processed_date > last_processed_date:
        # Read source tables from OLTP DB into a pandas DataFrame
        Billing = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."Billing"', engine)
        ClinicLocations = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."ClinicLocations"', engine)
        ClinicalReports = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."ClinicalReports"', engine)

        # Convert columns to datetime
        Billing['BillDate'] = pd.to_datetime(Billing['BillDate'])
        ClinicalReports['GeneratedDate'] = pd.to_datetime(ClinicalReports['GeneratedDate'])

        # Filter only the Incremental Data
        Billing = Billing[Billing['BillDate'] > last_processed_date]

        # Join tables to form the FactTransactions Table
        FactTransactions = pd.merge(Billing, ClinicLocations[['DisplayName', 'ClinicNbr']], how='left', left_on='Facility', right_on='DisplayName')
        FactTransactions = pd.merge(FactTransactions, ClinicalReports[['ReportRefNbr', 'DiagnosisCode']], how='left', left_on='DiagnosisReportRef', right_on='ReportRefNbr')
        FactTransactions = pd.merge(FactTransactions, ClinicalReports[['ReportRefNbr', 'CPTCode', 'GeneratedDate', 'CPTUnits']], how='left', left_on='ServiceReportRef', right_on='ReportRefNbr')

        # Rename Columns
        rename_dict = {
            'BillNbr': 'TransactionID',
            'PatientNbr': 'PatientID',
            'ProviderNbr': 'PhysicianID',
            'ClinicNbr': 'HospitalID',
            'DiagnosisCode': 'DiagnosisCodeID',
            'CPTCode': 'CPTCodeID',
            'GeneratedDate': 'ServiceDate',
            'BillDate': 'PaymentDate',
            'CPTUnits': 'CPTUnits',
            'BillAmount': 'BilledAmount',
            'InsuranceCoverage': 'InsurancePayment',
            'PaymentRecieved': 'PatientPayment'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())
        
        # Rename columns in the merged DataFrame
        FactTransactions = FactTransactions[selected_columns].rename(columns=rename_dict)

        # Write the new data to PostgreSQL table
        FactTransactions.to_sql("FactTransactions", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"FactTransactions has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for FactTransactions")

# Task 2: Transform Data to create DimPatient and upload to DB
def load_transform_dim_patient():
    # Check if there is new data in OLTP or not
    if max_processed_date > last_processed_date:
        # Read source tables from OLTP DB into a pandas DataFrame
        Patient = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."Patient"', engine)
        PatientSurvey = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."PatientSurvey"', engine)

        # Convert columns to datetime
        Patient['RegistrationDate'] = pd.to_datetime(Patient['RegistrationDate'])
        Patient['DOB'] = pd.to_datetime(Patient['DOB'])
        PatientSurvey['SurveyDate'] = pd.to_datetime(PatientSurvey['SurveyDate'])

        if last_processed_date > datetime.strptime('2022-01-01', '%Y-%m-%d'):
            # Filter the DataFrame based on the last processed date
            PatientSurvey = PatientSurvey[PatientSurvey['SurveyDate'] > last_processed_date]

        # Join tables to form the DimPatient Table
        DimPatient = pd.merge(Patient, PatientSurvey, how='right', left_on='PatientNbr', right_on='SurveyNbr')
        DimPatient['Age'] = (DimPatient['RegistrationDate'] - DimPatient['DOB']).dt.days // 365

        # Rename Columns
        rename_dict = {
            'PatientNbr': 'PatientID',
            'RegistrationDate': 'RegistrationDate',
            'HealthCardNbr': 'HealthCardNumber',
            'FirstName': 'FirstName',
            'LastName': 'LastName',
            'Email': 'Email',
            'Gender': 'Gender',
            'Age': 'Age',
            'DOB': 'DOB',
            'HeightCms': 'Height(cms)',
            'HeightIn': 'Height(in)',
            'WeightLbs': 'Weight(lbs)',
            'WeightKgs': 'Weight(kgs)',
            'BloodGroup': 'BloodGroup',
            'TobaccoUser': 'Tobacco',
            'AlcoholUser': 'Alcohol',
            'ExerciseFrequency': 'Exercise',
            'OnDiet': 'Diet',
            'Ethnicity': 'Ethinicity',
            'Address1': 'Address1',
            'Address2': 'Address2',
            'City': 'City',
            'State': 'State',
            'StateCode': 'StateCode',
            'Zip': 'ZipCode',
            'Region': 'Region'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())

        # Rename columns in the merged DataFrame
        DimPatient = DimPatient[selected_columns].rename(columns=rename_dict)

        # Clean FirstName
        DimPatient = DimPatient.apply(lambda row: row if pd.notnull(row['FirstName']) else (row.update({'FirstName': row['LastName'].split()[0], 'LastName': row['LastName'].split()[1]})) or row, axis=1)
        # Clean LastName
        DimPatient = DimPatient.apply(lambda row: row if pd.notnull(row['LastName']) else (row.update({'FirstName': row['FirstName'].split()[0], 'LastName': row['FirstName'].split()[1]})) or row, axis=1)
        # Clean Gender
        DimPatient['Gender'] = DimPatient['Gender'].replace({'F' : 'Female', 'M' : 'Male', 'O' : 'Other', 'U' : 'Unknown', 'UNK' : 'Unknown' })
        # Clean State
        DimPatient['State'] = DimPatient['State'].fillna(DimPatient['StateCode'])
        DimPatient['State'] = DimPatient['State'].replace({'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'})
        # Clean StateCode
        DimPatient['StateCode'] = DimPatient['StateCode'].fillna(DimPatient['State'])
        DimPatient['StateCode'] = DimPatient['StateCode'].replace({'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'})
        # Clean Height
        DimPatient['Height(cms)'] = DimPatient['Height(cms)'].fillna(round(DimPatient['Height(in)']/2.54))
        DimPatient['Height(in)'] = DimPatient['Height(in)'].fillna(round(DimPatient['Height(cms)']*2.54))
        # Clean Weight
        DimPatient['Weight(lbs)'] = DimPatient['Weight(lbs)'].fillna(round(DimPatient['Weight(kgs)']*2.2))
        DimPatient['Weight(kgs)'] = DimPatient['Weight(kgs)'].fillna(round(DimPatient['Weight(lbs)']/2.2))
        # Clean Exercise
        DimPatient['Exercise'] = DimPatient['Exercise'].replace({'Daily' : 'Yes', 'Weekly' : 'Yes', 'Bi-Weekly' : 'No', 'Monthly' : 'No', 'Ocassionally' : 'No'})

        # Write the new data to PostgreSQL table
        DimPatient.to_sql("DimPatient", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"DimPatient has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DimPatient")

# Task 3: Transform Data to create DimPhysician and upload to DB
def load_transform_dim_physician():
    # Check if there is new data in OLTP or not
    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Read source tables from OLTP DB into a pandas DataFrame
        Provider = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."Provider"', engine)
        ProviderSpecialty = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."ProviderSpecialty"', engine)

        # Join tables to form the DimPatient Table
        DimPhysician = pd.merge(Provider, ProviderSpecialty[['SpecialityCode','SpecialityType']], how='left', on='SpecialityCode')

        # Rename Columns
        rename_dict = {
            'ProviderNbr': 'ProviderID',
            'NpiNbr': 'NPI',
            'Prefix': 'Prefix',
            'FirstName': 'FirstName',
            'LastName': 'LastName',
            'Email': 'Email',
            'FTE': 'FTE',
            'ProviderCategory': 'ProviderType',
            'SpecialityType': 'SpecialityType'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())

        # Rename columns in the merged DataFrame
        DimPhysician = DimPhysician[selected_columns].rename(columns=rename_dict)

        # Write the new data to PostgreSQL table
        DimPhysician.to_sql("DimPhysician", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"DimPhysician has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DimPhysician")

# Task 4: Transform Data to create DimHospital and upload to DB
def load_transform_dim_hospital():
    # Check if there is new data in OLTP or not
    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Read source tables from OLTP DB into a pandas DataFrame
        ClinicLocations = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."ClinicLocations"', engine)

        # Form the DimHospital Table
        DimHospital = ClinicLocations.copy()

        # Rename Columns
        rename_dict = {
            'ClinicNbr': 'HospitalID',
            'DisplayName': 'HospitalName',
            'Address1': 'Address1',
            'Address2': 'Address2',
            'City': 'City',
            'State': 'State',
            'Zip': 'ZipCode',
            'Region': 'Region'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())

        # Rename columns in the merged DataFrame
        DimHospital = DimHospital[selected_columns].rename(columns=rename_dict)

        # Write the new data to PostgreSQL table
        DimHospital.to_sql("DimHospital", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"DimHospital has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DimHospital")

# Task 5: Transform Data to create DimDiagnosisCode and upload to DB
def load_transform_dim_diagnosis_code():
    # Check if there is new data in OLTP or not
    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Read source tables from OLTP DB into a pandas DataFrame
        DiagnosisCodeLookup = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."DiagnosisCodeLookup"', engine)

        # Form the DimDiagnosisCode Table
        DimDiagnosisCode = DiagnosisCodeLookup.copy()

        # Rename Columns
        rename_dict = {
            'DiagnosisCode': 'DiagnosisCode',
            'DiagnosisGrouping': 'DiagnosisCodeGroup',
            'DiagnosisDescription': 'DiagnosisCodeDescription'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())

        # Rename columns in the merged DataFrame
        DimDiagnosisCode = DimDiagnosisCode[selected_columns].rename(columns=rename_dict)

        # Write the new data to PostgreSQL table
        DimDiagnosisCode.to_sql("DimDiagnosisCode", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"DimDiagnosisCode has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DimDiagnosisCode")

# Task 6: Transform Data to create DimCPTCode and upload to DB
def load_transform_dim_cpt_code():
    # Check if there is new data in OLTP or not
    if last_processed_date == datetime.strptime('2022-01-01', '%Y-%m-%d'):
        # Read source tables from OLTP DB into a pandas DataFrame
        CptCodesLookup = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA_NAME}"."CptCodesLookup"', engine)

        # Form the DimCPTCode Table
        DimCPTCode = CptCodesLookup.copy()

        # Rename Columns
        rename_dict = {
            'CptCode': 'CPTCode',
            'CptGrouping': 'CPTCodeGroup',
            'CptDescription': 'CPTCodeDescription'
        }

        # Select only the columns that are being renamed
        selected_columns = list(rename_dict.keys())

        # Rename columns in the merged DataFrame
        DimCPTCode = DimCPTCode[selected_columns].rename(columns=rename_dict)

        # Write the new data to PostgreSQL table
        DimCPTCode.to_sql("DimCPTCode", engine, schema=TARGET_SCHEMA_NAME, if_exists="append", index=False)
        print(f"DimCPTCode has been uploaded to {TARGET_SCHEMA_NAME}")

    else:
        print(f"No new data to upload for DimCPTCode")

# Task 7: Update Last Processed Date Variable
def update_last_processed_date():
    # Update the last processed date
    Variable.set('OLAP_LAST_PROCESSED_DATE', max_processed_date.strftime('%Y-%m-%d'))


# Define tasks for each table
task_load_transform_fact_transactions = PythonOperator(
    task_id='load_transform_fact_transactions',
    python_callable=load_transform_fact_transactions,
    provide_context=True,
    task_display_name='Task 1: Transform and Upload FactTransactions Table to OLAP',
    dag=dag,
)

task_load_transform_dim_patient = PythonOperator(
    task_id='load_transform_dim_patient',
    python_callable=load_transform_dim_patient,
    provide_context=True,
    task_display_name='Task 2: Transform and Upload DimPatient Table to OLAP',
    dag=dag,
)

task_load_transform_dim_physician = PythonOperator(
    task_id='load_transform_dim_physician',
    python_callable=load_transform_dim_physician,
    provide_context=True,
    task_display_name='Task 3: Transform and Upload DimPhysician Table to OLAP',
    dag=dag,
)

task_load_transform_dim_hospital = PythonOperator(
    task_id='load_transform_dim_hospital',
    python_callable=load_transform_dim_hospital,
    provide_context=True,
    task_display_name='Task 4: Transform and Upload DimHospital Table to OLAP',
    dag=dag,
)

task_load_transform_dim_diagnosis_code = PythonOperator(
    task_id='load_transform_dim_diagnosis_code',
    python_callable=load_transform_dim_diagnosis_code,
    provide_context=True,
    task_display_name='Task 5: Transform and Upload DimDiagnosisCode Table to OLAP',
    dag=dag,
)

task_load_transform_dim_cpt_code = PythonOperator(
    task_id='load_transform_dim_cpt_code',
    python_callable=load_transform_dim_cpt_code,
    provide_context=True,
    task_display_name='Task 6: Transform and Upload DimCPTCode Table to OLAP',
    dag=dag,
)

task_update_last_processed_date = PythonOperator(
    task_id='update_last_processed_date',
    python_callable=update_last_processed_date,
    provide_context=True,
    task_display_name='Task 7: Update OLAP_LAST_PROCESSED_DATE Variable in Airflow',
    dag=dag,
)

# Define task dependencies to establish the order of execution
task_load_transform_fact_transactions >> task_load_transform_dim_patient >> task_load_transform_dim_physician >> \
task_load_transform_dim_hospital >> task_load_transform_dim_diagnosis_code >> task_load_transform_dim_cpt_code >> \
task_update_last_processed_date