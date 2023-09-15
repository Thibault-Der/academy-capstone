from pendulum import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(
    dag_id="connerctor-thibault",
    default_args={"owner": "Airflow"},
    schedule_interval="@daily",
    start_date=datetime(year=2023, month=9, day=15, tz="Europe/Brussels")
)

run_job = BatchOperator(
    task_id="connector-thibault",
    dag=dag,
    job_name="Thibault_job",
    job_definition="arn:aws:batch:eu-west-1:338791806049:job-definition/Thibault_job:1",
    job_queue="arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-summer-2023-job-queue",
    overrides={}
)
run_job