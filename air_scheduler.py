from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from polygonio_extract import fetch_and_upload_stock_data

# Define the default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'TSBurris',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 16),  # Change to your desired start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule': None, # Turn off the scheduler by setting it to None
}

# Create a DAG instance
dag = DAG(
    'fetch_and_upload_stock_data',
    default_args=default_args,
    # schedule_interval=timedelta(days=1),  # Adjust the interval as needed
)

# Define a PythonOperator to run the script
run_script_task = PythonOperator(
    task_id='run_script',
    python_callable=fetch_and_upload_stock_data,  # Use the function name from your script
    dag=dag,
)

# Set the task dependencies (if any)
# You can use >> and << operators to define dependencies between tasks
# For example, run_script_task >> another_task

if __name__ == "__main__":
    dag.cli()
