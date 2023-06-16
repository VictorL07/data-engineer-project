from airflow import models
import json
import datetime
from airflow.models import Variable
from email.encoders import encode_base64
import logging
from google.cloud import pubsub
import os
from airflow.operators import dummy_operator, bash_operator, python_operator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

project_id                  = Variable.get('gcp_project_name')
country                     = Variable.get('country').upper()
environment                 = Variable.get('environment')
bucket_list                 = Variable.get("bucket_names", deserialize_json=True)
dataset_name                = Variable.get("dataset_name")
dateFolder                  = datetime.datetime.now().strftime("%Y%m%d")

id_reporte                  = '{{dag_run.conf["value_2"]}}'
file_ext                    = '{{dag_run.conf["value_3"]}}'
delimiter                   = '{{dag_run.conf["value_4"]}}'
input_report_path           = '{{dag_run.conf["value_5"]}}'
output_report_bucket        = '{{dag_run.conf["value_6"] | replace("YYYYMMDD",'+dateFolder+')}}'
footer_value                = '{{dag_run.conf["value_7"]}}' #No include footer if footer value has 'false' value
send_email                  = '{{dag_run.conf["value_8"]}}' #If only create a file this value is 'false' else 'true'
subject_email               = '{{dag_run.conf["value_9"]}}' #Subject of email when send_email value is True
message_email               = '{{dag_run.conf["value_10"]}}' #Message of email when send_email value is True
has_attachment              = '{{dag_run.conf["value_11"]}}' #If send email without file attachment is 'false' else 'true'
key_receiver_address            = '{{dag_run.conf["value_12"]}}' #Find the list in email dict (airflow_variables), this value is the key

header_tmp_table            = 'tmp_header'+'_'+id_reporte
body_tmp_table              = 'tmp_body'+'_'+id_reporte
footer_tmp_table            = 'tmp_footer'+'_'+id_reporte
report_name_tmp_table       = 'tmp_report_name'+'_'+id_reporte
composer_path               = '/home/airflow/gcs/data/reports/'+id_reporte
input_blob                  = "/preprocess/report_name"
smtp_credentials            = Variable.get("smtp_credentials", deserialize_json=True)
today                       = datetime.datetime.now()

cross_project               = Variable.get("cross_project")
cross_dataset               = Variable.get("cross_dataset")

default_dag_args = {
    'owner': 'Unknow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(today.year, today.month, today.day),
    'schedule_interval': None
}

with models.DAG('generate-reports', default_args=default_dag_args, schedule_interval=None) as dag:

    start           = dummy_operator.DummyOperator(task_id='start')
    end             = dummy_operator.DummyOperator(task_id='end',trigger_rule=TriggerRule.ONE_SUCCESS,)   
    checkpoint_send_email    = dummy_operator.DummyOperator(task_id='checkpoint_send_email',trigger_rule=TriggerRule.ONE_SUCCESS,)

    def x_push(task_instance, key_data, value):
        logging.info('key_data:' + key_data)
        logging.info('value:' + value)
        msg = task_instance.xcom_push(key=key_data, value=value)
        logging.info('=====================================================================')

    def get_env_email_subject_prefix():
        country = Variable.get('country').upper()
        environment = Variable.get('environment').upper()

        if environment == 'PROD':
            return '[{}] | '.format(country)
        else:
            return '[{}] [{}] | '.format(country, environment)

    def get_data_from_report_name(**kwargs):
        hook = BigQueryHook(bigquery_conn_id='bigquery_default', delegate_to=None, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM "+dataset_name+".tmp_report_name_"+kwargs['params']['value_2'])
        bq_report_name = cursor.fetchall()
        print('result: ', bq_report_name[0][0])
        x_push(kwargs['task_instance'], 'bq_report_name', str(bq_report_name[0][0]))
        return bq_report_name

    def send_email_from_gmail(**kwargs):
        logging.info('start email')
        list_emails = Variable.get("email", deserialize_json=True)
        receiver_address = list_emails[kwargs['params']['value_12']]
        logging.info('send_email:'+ kwargs['params']['value_8'])
        logging.info('subject_email:'+ kwargs['params']['value_9'])
        logging.info('message_email:'+ kwargs['params']['value_10'])
        logging.info('has_attachment:'+ kwargs['params']['value_11'])
        logging.info('output_report_bucket:'+ kwargs['params']['value_6'])
        logging.info('key_receiver_address:'+ kwargs['params']['value_12'])

        subject_email = "[{0}][{1}]".format(country,environment.upper())+kwargs['params']['value_9'].replace("YYYYMMDD",dateFolder)
        message_email = kwargs['params']['value_10']
        has_attachment = kwargs['params']['value_11']
        output_report_bucket = kwargs['params']['value_6'].replace("YYYYMMDD",dateFolder)
        SUBJECT = subject_email
        TEXT = message_email
        FILE_NAME = kwargs['task_instance'].xcom_pull(key='bq_report_name', task_ids='t11_get_data_from_report_name') + kwargs['params']['value_3']
        ATTACHMENT = output_report_bucket if has_attachment.__eq__("true") else ''
        logging.info('ATTACHMENT:' + ATTACHMENT)

        logging.info("preparing to send")
        try:
            email_message_pubsub = {"receiver_email":receiver_address, "subject":SUBJECT, "message":TEXT, "has_attachment":has_attachment, "attachment": ATTACHMENT, "report_name": FILE_NAME}
            email_json = json.dumps(email_message_pubsub)
            data = email_json.encode('utf-8')
            publisher = pubsub.PublisherClient()
            topic_name = "{}-{}-tcp-send-email-39bf0aa48a88".format(country.lower(),environment)
            topic_path = publisher.topic_path(project_id,topic_name)
            future = publisher.publish(topic_path, data)
            message_id = future.result()
            if(message_id):
                logging.info("Pubsub future result: " + message_id)
            logging.info("email has been sent")
        except:
            logging.info("Error sending email")
    
    def check_send_email(**kwargs):
        logging.info('send_email:' + kwargs['params']['value_8'])
        send_email = kwargs['params']['value_8']
        if send_email.__eq__("true"):
            return 't13_send_email'
        else:
            return 'checkpoint_send_email'

    t1_bq_header_loading   = BashOperator(
                    task_id='t1_bq_header_loading',
                    bash_command=f'gsutil cp {input_report_path}/header.sql . \
                    && sed -i "s/cross_project/{cross_project}/g" header.sql \
                    && sed -i "s/cross_dataset/{cross_dataset}/g" header.sql \
                    && bq query \
                    --destination_table {dataset_name}.{header_tmp_table} \
                    --use_legacy_sql=false \
                    --replace \
                    --flagfile=header.sql && rm header.sql',
    )

    t2_bq_body_loading   = BashOperator(
                    task_id='t2_bq_body_loading',
                    bash_command=f'gsutil cp {input_report_path}/body.sql . \
                    && sed -i "s/cross_project/{cross_project}/g" body.sql \
                    && sed -i "s/cross_dataset/{cross_dataset}/g" body.sql \
                    && bq query \
                    --destination_table {dataset_name}.{body_tmp_table} \
                    --use_legacy_sql=false \
                    --replace \
                    --flagfile=body.sql && rm body.sql',
    )

    t3_bq_footer_loading   = BashOperator(
                    task_id='t3_bq_footer_loading',
                    bash_command=f'gsutil cp {input_report_path}/footer.sql . && bq query \
                    --destination_table {dataset_name}.{footer_tmp_table} \
                    --use_legacy_sql=false \
                    --replace \
                    --flagfile=footer.sql && rm footer.sql',
    )

    t4_bq_report_name_loading   = BashOperator(
                    task_id='t4_bq_report_name_loading',
                    bash_command=f'gsutil cp {input_report_path}/report_name.sql . && bq query \
                    --destination_table {dataset_name}.{report_name_tmp_table} \
                    --use_legacy_sql=false \
                    --replace \
                    --flagfile=report_name.sql && rm report_name.sql',
    )

    t5_export_header_to_gcs   = BashOperator(
                task_id='t5_export_header_to_gcs',
                bash_command=f'bq extract \
                                --destination_format CSV \
                                --field_delimiter=\'{delimiter}\' \
                                --print_header=False \
                                {dataset_name}.{header_tmp_table} \
                                {output_report_bucket}/preprocess/header{file_ext}'
    )

    t6_export_body_to_gcs   = BashOperator(
                task_id='t6_export_body_to_gcs',
                bash_command=f'bq extract \
                                --destination_format CSV \
                                --field_delimiter=\'{delimiter}\' \
                                --print_header=False \
                                {dataset_name}.{body_tmp_table} \
                                {output_report_bucket}/preprocess/body{file_ext}'
    )

    t7_export_footer_to_gcs   = BashOperator(
                task_id='t7_export_footer_to_gcs',
                bash_command=f'bq extract \
                                --destination_format CSV \
                                --field_delimiter=\'{delimiter}\' \
                                --print_header=False \
                                {dataset_name}.{footer_tmp_table} \
                                {output_report_bucket}/preprocess/footer{file_ext}'
    )

    t8_export_report_name_to_gcs   = BashOperator(
                task_id='t8_export_report_name_to_gcs',
                bash_command=f'bq extract \
                                --destination_format CSV \
                                --field_delimiter=\'{delimiter}\' \
                                --print_header=False \
                                {dataset_name}.{report_name_tmp_table} \
                                {output_report_bucket}/preprocess/report_name{file_ext}'
    )

    t9_join_reports = bash_operator.BashOperator(
        task_id='t9_join_reports',
        bash_command="""tmp_bq_report_name=$(bq query --nouse_legacy_sql --format=csv \
                        'SELECT * FROM `{dataset_name}.{report_name_tmp_table}`') \
                        && split_result=($tmp_bq_report_name) && tmp_bq_report_name=${result} \
                        && gsutil cat {output_report_bucket}/preprocess/header{file_ext}  \
                        {output_report_bucket}/preprocess/body{file_ext} \
                        $(if [ '{footer_value}' = 'true' ]; then echo {output_report_bucket}/preprocess/footer{file_ext}; fi) >> $tmp_bq_report_name{file_ext} \
                        && sed -i 's/|//g;s/"//g' $tmp_bq_report_name{file_ext} && gsutil mv $tmp_bq_report_name{file_ext} \
                        {output_report_bucket}/$tmp_bq_report_name{file_ext}""".format(
                            file_ext=file_ext, 
                            output_report_bucket=output_report_bucket, 
                            dataset_name=dataset_name,
                            report_name_tmp_table=report_name_tmp_table, 
                            result='{split_result[1]}',#|cd| = custom delimiter
                            footer_value=footer_value
                            ), 
        dag=dag
    )

    t10_delete_tmp_data = bash_operator.BashOperator(
        task_id='t10_delete_tmp_data',
        trigger_rule='all_done',
        bash_command="""gsutil -m rm -R {output_report_bucket}/preprocess""".format(output_report_bucket=output_report_bucket),
        dag=dag
    )

    t11_get_data_from_report_name = python_operator.PythonOperator(
        task_id='t11_get_data_from_report_name',
        provide_context=True,
        python_callable=get_data_from_report_name,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    t12_check_send_email = python_operator.BranchPythonOperator(
        task_id="t12_check_send_email",
        python_callable=check_send_email,
        provide_context=True,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    t13_send_email = python_operator.PythonOperator(
        task_id='t13_send_email',
        provide_context=True,
        python_callable=send_email_from_gmail,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )
    
t1_bq_header_loading >> t2_bq_body_loading >> t3_bq_footer_loading >> t4_bq_report_name_loading >> t5_export_header_to_gcs >> t6_export_body_to_gcs >> t7_export_footer_to_gcs >> t8_export_report_name_to_gcs >> t9_join_reports >> t10_delete_tmp_data >> t11_get_data_from_report_name >> t12_check_send_email
t12_check_send_email >> checkpoint_send_email
t12_check_send_email >> t13_send_email
t13_send_email >> end
checkpoint_send_email >> end
