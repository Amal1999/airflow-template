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
    gdrive_hook = GoogleDriveHook(gcp_conn_id='google_cloud_default')
    file_id = '1RZQhhr8ff1WWZ0c2Mo5G-4LTwJZez9Bw'
    file_handle = BytesIO()
    gdrive_hook.download_file(file_id=file_id, file_handle=file_handle)
    file_handle.seek(0)
    file_content = file_handle.read().decode('utf-8')
    return file_content

def feature_selection(**kwargs):
    fetched_data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_drive')
    
    df = pd.read_csv(BytesIO(fetched_data.encode('utf-8')))
    
    selected_features = df[['Airline Name', 'Overall_Rating', 'Review_Title', 'Review Date', 'Review', 'Recommended']]


    selected_features_str = selected_features.to_csv(index=False)

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
    
    train_str = X_train.to_csv(index=False) + '\n' + y_train.to_csv(index=False)
    val_str = X_val.to_csv(index=False) + '\n' + y_val.to_csv(index=False)
    test_str = X_test.to_csv(index=False) + '\n' + y_test.to_csv(index=False)
    
    return train_str, val_str, test_str


from googleapiclient.http import MediaIoBaseUpload

def upload_splitted_data_to_drive(**kwargs):
    local_paths = kwargs['ti'].xcom_pull(task_ids='split_data')
    service = GoogleDriveHook(gcp_conn_id='google_cloud_default').get_conn()
    
    for i, data in enumerate(local_paths):
        local_location = f"/tmp/splitted_data_{i}.csv"  # Adjust the local location as per your requirement
        with open(local_location, 'w') as file:
            file.write(data)
        
        remote_location = f"1bQoVXCAI3fHTX9k8qdvSO2cLMZCiCUFP/splitted_data_{i}.csv"  # Adjust the remote location
        file_metadata = {
            'name': f'splitted_data_{i}.csv',
            'parents': [remote_location],
        }
        media = MediaIoBaseUpload(local_location, mimetype='text/csv')
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info("File %s uploaded to Google Drive at %s.", local_location, remote_location)





dag = DAG(
    'fetch_and_preprocess_data_from_gdrive',
    default_args=default_args,
    description='A DAG to fetch data from Google Drive, preprocess it, and perform feature selection',
    schedule_interval=timedelta(days=1),
)

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

upload_splitted_data_task = PythonOperator(
    task_id='upload_splitted_data_to_drive',
    python_callable=upload_splitted_data_to_drive,
    provide_context=True,
    dag=dag,
)



fetch_data_task >> feature_selection_task >> split_data_task >> upload_splitted_data_task
