import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='run_scripts',  # Add DAG_Id here
    description='DAG to run Main.py',
    schedule_interval=None,
    start_date=datetime(2024, 3, 17),
    catchup=False
)

# Define the task
def run_main():
    try:
        main_path = os.path.join('/app', 'Main.py')
        exec(open(main_path).read())
        
        # Call Function.py here
        function_path = os.path.join('/app', 'Function.py')
        exec(open(function_path).read())
        
    except Exception as e:
        # Handle the exception here
        print(f"Error occurred while running Main.py: {str(e)}")

task_run_main = PythonOperator(
    task_id='run_main',
    python_callable=run_main,
    dag=dag,
)

# Define the task dependency
task_run_main
