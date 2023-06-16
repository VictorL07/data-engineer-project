import datetime
from airflow.models import Variable
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators import dummy_operator, bash_operator, python_operator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import logging
import uuid


REGION = "us-east1"

project_id                  = Variable.get('gcp_project_name')
country                     = Variable.get('country')
environment                 = Variable.get('environment')
smtp_list                   = Variable.get("smtp_credentials", deserialize_json=True)
receiver_email              = smtp_list['receiver_address']
today                       = datetime.datetime.now()


def x_push(task_instance, key_data, value):
    logging.info('key_data:' + key_data)
    logging.info('value:' + str(value))
    msg = task_instance.xcom_push(key=key_data, value=value)
    logging.info('=====================================================================')

def start(**kwargs):
    input_location = kwargs['dag_run'].conf.get('input_source')
    output_location =  kwargs['dag_run'].conf.get('output_source')
    input_flag = kwargs['dag_run'].conf.get('input_flag')
    output_flag = kwargs['dag_run'].conf.get('output_flag')
    python_file_location = kwargs['dag_run'].conf.get('pyspark_file')
    jars_location = kwargs['dag_run'].conf.get('jars_file')

    x_push(kwargs['task_instance'], 'python_file_location', python_file_location)
    x_push(kwargs['task_instance'], 'jars_location', jars_location)

    logging.info("Variables")
    logging.info(input_location)
    logging.info(output_location)
    logging.info(input_flag)
    logging.info(output_flag)
    logging.info(python_file_location)
    logging.info(jars_location)

    x_push(kwargs['task_instance'], 'project_id', project_id)
    x_push(kwargs['task_instance'], 'country', country)
    x_push(kwargs['task_instance'], 'environment', environment)
    x_push(kwargs['task_instance'], 'receiver_email', receiver_email)
    x_push(kwargs['task_instance'], 'input_location', input_location)
    x_push(kwargs['task_instance'], 'output_location', output_location)
    x_push(kwargs['task_instance'], 'input_flag', input_flag)
    x_push(kwargs['task_instance'], 'output_flag', output_flag)


def choose_dataproc(**kwargs):
    logging.info('jars_location:' + kwargs['dag_run'].conf.get('jars_file'))
    jars_location = kwargs['dag_run'].conf.get('jars_file')
    if jars_location == '':
        return 'create_batch'
    else:
        return 'create_batch_w_jar'

default_dag_args = {
    'owner': 'Unknow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(today.year, today.month, today.day),
    'schedule_interval': None,
    "region": REGION
}

    
with models.DAG('generate-dataproc', default_args=default_dag_args, schedule_interval=None) as dag:

   
    start = python_operator.PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=start,
    retries=0,
    dag=dag)
    end             = dummy_operator.DummyOperator(task_id='end',trigger_rule=TriggerRule.ONE_SUCCESS,)   

    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ ti.xcom_pull(key='python_file_location',task_ids='start') }}",
                "args": ["--project_id={{ ti.xcom_pull(key='project_id',task_ids='start') }}",
                         "--country={{ ti.xcom_pull(key='country',task_ids='start') }}",
                         "--environment={{ ti.xcom_pull(key='environment',task_ids='start') }}",
                         "--receiver_email={{ ti.xcom_pull(key='receiver_email',task_ids='start') }}",
                         "--input_file={{ ti.xcom_pull(key='input_location',task_ids='start') }}",
                         "--output_file={{ ti.xcom_pull(key='output_location',task_ids='start') }}",
                         "--input_flag={{ ti.xcom_pull(key='input_flag',task_ids='start') }}",
                         "--output_flag={{ ti.xcom_pull(key='output_flag',task_ids='start') }}"]
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "projects/{{ ti.xcom_pull(key='project_id',task_ids='start') }}/regions/us-east1/subnetworks/dataproc-subnetwork",
                },
            },
        },
        batch_id="batch-generic-dataproc-" + str(uuid.uuid4()).split("-")[-1],
    )

    create_batch_w_jar = DataprocCreateBatchOperator(
        task_id="create_batch_w_jar",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ ti.xcom_pull(key='python_file_location',task_ids='start') }}",
                "jar_file_uris": ["{{ ti.xcom_pull(key='jars_location',task_ids='start') }}"],
                "args": ["--project_id={{ ti.xcom_pull(key='project_id',task_ids='start') }}",
                         "--country={{ ti.xcom_pull(key='country',task_ids='start') }}",
                         "--environment={{ ti.xcom_pull(key='environment',task_ids='start') }}",
                         "--receiver_email={{ ti.xcom_pull(key='receiver_email',task_ids='start') }}",
                         "--input_file={{ ti.xcom_pull(key='input_location',task_ids='start') }}",
                         "--output_file={{ ti.xcom_pull(key='output_location',task_ids='start') }}",
                         "--input_flag={{ ti.xcom_pull(key='input_flag',task_ids='start') }}",
                         "--output_flag={{ ti.xcom_pull(key='output_flag',task_ids='start') }}"]
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "projects/{{ ti.xcom_pull(key='project_id',task_ids='start') }}/regions/us-east1/subnetworks/dataproc-subnetwork",
                },
            },
        },
        batch_id="batch-generic-dataproc-" + str(uuid.uuid4()).split("-")[-1],
    )

    choose_option_dataproc = python_operator.BranchPythonOperator(
        task_id="choose_option_dataproc",
        python_callable=choose_dataproc,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )
    
    start >> choose_option_dataproc 
    choose_option_dataproc >> create_batch
    choose_option_dataproc >> create_batch_w_jar
    create_batch >> end
    create_batch_w_jar >> end
