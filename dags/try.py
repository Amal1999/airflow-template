from io import BytesIO
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gdrive_hook import GoogleDriveHook
import pandas as pd

logger = logging.getLogger(__name__)
# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 4)
}

# Function to fetch data from Google Drive
def fetch_data_from_drive():
    gdrive_hook = GoogleDriveHook(gcp_conn_id='google_drive_default')
    file_id = '1RZQhhr8ff1WWZ0c2Mo5G-4LTwJZez9Bw'
    file_handle = BytesIO()
    gdrive_hook.download_file(file_id=file_id, file_handle=file_handle)
    file_handle.seek(0)
    file_content = file_handle.read().decode('utf-8')
    return file_content

# Function to preprocess the data (just for test purposes)
def preprocess_data(**kwargs):
    # Get the file content from the previous task's context
    file_content = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_drive')

    # Assuming the data is CSV
    df = pd.read_csv(BytesIO(file_content.encode('utf-8')))
    
    # Your preprocessing steps here
    # Example: df.dropna(inplace=True)
    
    # Convert DataFrame to string
    preprocessed_data_str = df.to_csv(index=False)
    
    return preprocessed_data_str

# Function for feature selection
def feature_selection(**kwargs):
    # Get the preprocessed data from the previous task's context
    preprocessed_data_str = kwargs['ti'].xcom_pull(task_ids='preprocess_data')
    
    # Convert preprocessed data string to DataFrame
    df = pd.read_csv(BytesIO(preprocessed_data_str.encode('utf-8')))
    
    # Select specific features
    selected_features = df[['Airline Name', 'Overall_Rating', 'Review_Title', 'Review Date', 'Review']]
    
    # Convert selected features to string
    selected_features_str = selected_features.to_string(index=False)
    
    return selected_features_str

# Define the DAG
dag = DAG(
    'fetch_and_preprocess_data_from_gdrive',
    default_args=default_args,
    description='A DAG to fetch data from Google Drive, preprocess it, and perform feature selection',
    schedule_interval=timedelta(days=1),
)

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_drive',
    python_callable=fetch_data_from_drive,
    dag=dag,
)

preprocess_data_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    provide_context=True,
    dag=dag,
)

feature_selection_task = PythonOperator(
    task_id='feature_selection',
    python_callable=feature_selection,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_data_task >> preprocess_data_task >> feature_selection_task
