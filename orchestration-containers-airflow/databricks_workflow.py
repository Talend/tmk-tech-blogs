"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
import requests
import base64
import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.python_operator import PythonOperator
from slackclient import SlackClient
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator

DOMAIN = ''
TOKEN = b''
TOKEN_BASIC = ''
DATABRICKS_ENDPOINT = ''


SLACK_TOKEN = ''
SLACK_USERNAME = 'Airflow'
SLACK_CHANNEL = '#databricks_jobs'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'concurrency': 1,
    'max_active_runs': 1
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

"""
FUNCTIONS
"""

def getStatus(domain, token, cluster):
    response = requests.get(
      'https://%s/api/2.0/clusters/get' % (domain),
      headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + token)},
      json={
        "cluster_id": cluster
      }
    )
    if response.status_code == 200:
      ret = response.json()['state']
    else:
      ret =  "Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"])
    print("Cluster Status : " + ret)
    return ret

def create_databricks_cluster(**kwargs):
    response = requests.post(
      'https://%s/api/2.0/clusters/create' % (DOMAIN),
      headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + TOKEN)},
      json={
          "cluster_name": "databricks-cluster",
          "spark_version": "3.5.x-scala2.11",
          "node_type_id": "Standard_D3_v2",
          "num_workers": 1,
          'spark_env_vars': {
            'PYSPARK_PYTHON': '/databricks/python3/bin/python3',
            }
      }
    )
    print(response.json()['cluster_id'])
    status = "PENDING"
    while status != "RUNNING":
        status = getStatus(DOMAIN, TOKEN, response.json()['cluster_id'])
        time.sleep(5)
    return response.json()['cluster_id']
    

def terminate_databricks_cluster(clusterid):
    response = requests.post(
      'https://%s/api/2.0/clusters/permanent-delete' % (DOMAIN),
      headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + TOKEN)},
      json={
        "cluster_id": "{{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}"
      }
    )
    print(response.json())

dag = DAG('databricks_workflow', schedule_interval="@once", default_args=default_args)

"""
TASKS
"""

create_cluster = PythonOperator(
    task_id='create_databricks_cluster',
    dag=dag,
    python_callable=create_databricks_cluster
)


create_cluster_notify = SlackAPIPostOperator(
    task_id='create_cluster_notify',
    username  = 'Airflow',
    token='xoxp-2953391458-240323533415-487123270576-d2bb3db1dc0b0e9802c07645e8c98d0b',
    channel='#databricks_jobs',
    text=":databricks: Databricks Cluster Created with ID: {{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}",
    dag=dag
)

create_cluster_notify.set_upstream(create_cluster)

train_model = ECSOperator(
              task_id="train_model",
              task_definition='trainmodelriskassessment',
              cluster='TalendECS',
              aws_conn_id='aws_default',
              overrides={
                  'containerOverrides': [
                          {
                              'name': "trainmodelriskassessment",
                              'command': [
                                  "--context_param DATABRICKS_ENDPOINT=XXX",
                                  "--context_param DATABRICKS_TOKEN=XXX",
                                  "--context_param DATABRICKS_CLUSTER_ID={{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}"
                              ]
                          },
                      ]
              },
              region_name='us-east-1',
              launch_type='EC2',
              dag=dag
          )


# define list of lobs we want to run for
# loop through the lob's we want to use to build up our dag


s3_list_files = S3ListOperator(
              task_id="s3_list_files",
              bucket="tgourdel-storage",
              aws_conn_id="aws_default",
              dag=dag
)

s3_list_files.set_upstream(create_cluster_notify)

files = s3_list_files.execute(None)
for x in files:
  run_job = ECSOperator(
              task_id="Copy_%s_to_DBFS" % (x),
              task_definition='uploadtodbfs',
              cluster='TalendECS',
              aws_conn_id='aws_default',
              overrides={
                  'containerOverrides': [
                          {
                              'name': "uploadtodbfs",
                              'command': [
                                  "--context_param DATABRICKS_ENDPOINT=XXX",
                                  "--context_param DATABRICKS_TOKEN=XXX",
                                  "--context_param AWS_ACCESS_KEY=XXX",
                                  "--context_param AWS_SECRET_KEY=XXX",
                                  "--context_param AWS_BUCKET=XXX",
                                  "--context_param AWS_FILE=%s.csv" % (x)
                              ]
                          },
                      ]
              },
              region_name='us-east-1',
              launch_type='EC2',
              dag=dag
          )
  run_job_notify = SlackAPIPostOperator(
      task_id="Copy_%s_notify" % (x),
      username  = 'Airflow',
      token='XXX',
      channel='#databricks_jobs',
      text=':ecs: Talend Containerized Job : Copy %s from S3 TO DBFS successful!' % (x),
      dag=dag
  )
  run_job.set_upstream(s3_list_files)
  run_job_notify.set_upstream(run_job)
  train_model.set_upstream(run_job_notify)




train_model_notify = SlackAPIPostOperator(
      task_id="train_model_notify",
      username  = 'Airflow',
      token='XXX',
      channel='#databricks_jobs',
      text=':ecs: Talend Containerized Databricks Job : Train model successful',
      dag=dag
)

train_model_notify.set_upstream(train_model)

test_model = ECSOperator(
              task_id="test_model",
              task_definition='testmodelriskassessment',
              cluster='TalendECS',
              aws_conn_id='aws_default',
              overrides={
                  'containerOverrides': [
                          {
                              'name': "testmodelriskassessment",
                              'command': [
                                  "--context_param DATABRICKS_ENDPOINT=XXX",
                                  "--context_param DATABRICKS_TOKEN=XXX",
                                  "--context_param DATABRICKS_CLUSTER_ID={{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}"
                              ]
                          },
                      ]
              },
              region_name='us-east-1',
              launch_type='EC2',
              dag=dag
          )

test_model.set_upstream(train_model_notify)


test_model_notify = SlackAPIPostOperator(
      task_id="test_model_notify",
      username  = 'Airflow',
      token='XXX',
      channel='#databricks_jobs',
      text=':ecs: Talend Containerized Databricks Job : Test model successful',
      dag=dag
)

test_model_notify.set_upstream(test_model)

terminate_cluster = PythonOperator(
      task_id='terminate_databricks_cluster',
      dag=dag,
      python_callable=terminate_databricks_cluster,
      op_kwargs={"clusterid": "{{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}"},
)

terminate_cluster.set_upstream(test_model_notify)

terminate_cluster_notify = SlackAPIPostOperator(
    task_id='terminate_cluster_notify',
    username  = SLACK_USERNAME,
    token = SLACK_TOKEN,
    channel= SLACK_CHANNEL,
    text=":black_circle: Databricks Cluster terminated (Cluster ID: {{ task_instance.xcom_pull(task_ids='create_databricks_cluster') }}",
    dag=dag
)
terminate_cluster_notify.set_upstream(terminate_cluster)


