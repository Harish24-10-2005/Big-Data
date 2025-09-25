from airflow import DAG
# BashOperator (AF 2.x / 1.10.x)
try:
    from airflow.operators.bash import BashOperator
except Exception:
    from airflow.operators.bash_operator import BashOperator
# DummyOperator for Airflow 2.2 (EmptyOperator not available until 2.3.0)
try:
    from airflow.operators.dummy import DummyOperator as EmptyOperator
except Exception:
    from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
# days_ago fallback (AF 1.10.x)
try:
    from airflow.utils.dates import days_ago
except Exception:
    from datetime import datetime, timedelta
    def days_ago(n): return datetime.utcnow() - timedelta(days=n)
from airflow.utils.trigger_rule import TriggerRule
import os

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

STREAM_WINDOW_SECS = int(os.environ.get("STREAM_WINDOW_SECS", "180"))
USE_TIMEOUT = os.environ.get("USE_TIMEOUT", "1") == "1"

def maybe_timeout(cmd: str) -> str:
    return f"timeout {STREAM_WINDOW_SECS}s {cmd}" if USE_TIMEOUT else cmd

dag = DAG(
    'ott_pipeline_dag',
    default_args=default_args,
    description='OTT: Producers -> Consumers -> Lake -> Transform -> MySQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)

start = EmptyOperator(task_id='start', dag=dag)

# Producers
producer_paths = {
    'amazon': '/home/hadoop/project/producers/amazon_producer.py',
    'netflix': '/home/hadoop/project/producers/netflix_producer.py',
    'hulu': '/home/hadoop/project/producers/hulu_producer.py',
    'disney': '/home/hadoop/project/producers/disney_producer.py'
}

producer_tasks = {
    platform: BashOperator(
        task_id=f'{platform}_producer',
        bash_command=f'python3 {path}',
        dag=dag
    ) for platform, path in producer_paths.items()
}

join_producers = EmptyOperator(task_id='join_producers', trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)

# Consumers (CORRECTED)
consumer_titles = BashOperator(
    task_id='consume_titles',
    bash_command=maybe_timeout('spark-submit --master local[2] /home/hadoop/project/consumer/consumer_titles.py'),
    dag=dag
)

consumer_credits = BashOperator(
    task_id='consume_credits',
    bash_command=maybe_timeout('spark-submit --master local[2] /home/hadoop/project/consumer/consumer_credits.py'),
    dag=dag
)

# CRITICAL FIX: Add the missing join_consumers task
join_consumers = EmptyOperator(task_id='join_consumers', trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)

# Transform (CORRECTED)
spark_transform = BashOperator(
    task_id='spark_clean_transform',
    bash_command='spark-submit --master local[*] --deploy-mode client /home/hadoop/project/jobs/spark_clean_transform.py',
    dag=dag
)

# Load (CORRECTED)  
load_mysql = BashOperator(
    task_id='load_curated_to_mysql',
    bash_command='spark-submit --master local[*] --deploy-mode client '
                 '--packages mysql:mysql-connector-java:8.0.33 /home/hadoop/project/jobs/load_to_mysql.py',
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

# DAG dependencies
start >> list(producer_tasks.values()) >> join_producers
join_producers >> [consumer_titles, consumer_credits] >> join_consumers
join_consumers >> spark_transform >> load_mysql >> end
