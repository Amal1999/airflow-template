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

def fetch_data_from_drive():
    gdrive_hook = GoogleDriveHook(gcp_conn_id='google_drive_default')
    file_id = '1RZQhhr8ff1WWZ0c2Mo5G-4LTwJZez9Bw'
    file_handle = BytesIO()
    gdrive_hook.download_file(file_id=file_id, file_handle=file_handle)
    file_handle.seek(0)
    file_content = file_handle.read().decode('utf-8')
    return file_content

def feature_selection(**kwargs):
    # Get the fetched data from the previous task's context
    fetched_data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_drive')
    
    # Assuming the data is CSV
    df = pd.read_csv(BytesIO(fetched_data.encode('utf-8')))
    
    # Select specific features
    selected_features = df[['Airline Name', 'Overall_Rating', 'Review_Title', 'Review Date', 'Review', 'Recommended']]

    # Convert selected_features to string
    selected_features_str = selected_features.to_string(index=False)

    return selected_features_str


def split_data(**kwargs):
    from sklearn.model_selection import train_test_split
    selected_data_str = kwargs['ti'].xcom_pull(task_ids='feature_selection')
    
    df = pd.read_csv(BytesIO(selected_data_str.encode('utf-8')))
    
    # Split the data into features and target
    X = df[['Airline Name', 'Overall_Rating', 'Review_Title', 'Review Date', 'Review']]
    y = df['Recommended']
    
    # Split the data into training (70%), validation (15%), and test (15%) sets
    X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42)
    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)
    
    # Convert sets to CSV string format
    train_str = X_train.to_csv(index=False) + '\n' + y_train.to_csv(index=False)
    val_str = X_val.to_csv(index=False) + '\n' + y_val.to_csv(index=False)
    test_str = X_test.to_csv(index=False) + '\n' + y_test.to_csv(index=False)
    
    return train_str, val_str, test_str


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


feature_selection_task = PythonOperator(
    task_id='feature_selection',
    python_callable=feature_selection,
    provide_context=True,
    dag=dag,
)

split_data_task = PythonOperator(
    task_id='split_data',
    python_callable=split_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_data_task >> feature_selection_task >> split_data_task
