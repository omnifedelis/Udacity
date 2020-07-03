from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import (StageToRedshiftOperator, DataQualityOperator)



default_args = {
    'owner': 'Garcia_Capstone_DE',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2016, 1, 1),
}

dag = DAG('Capstone_dag2',
          default_args=default_args,
          description='Use Airflow to load and transform data with Spark and Query in Redshift',
          catchup=False,
          schedule_interval='0 * * * *'
)



CAP_EMR_Steps=[
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Copy_ETL_Script',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://garcia-capstone/scripts/', '/home/hadoop/', '--recursive']
        }
    },
  
    {
        'Name': 'Main_ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/CAP_Spark_Wrangling.py' , 's3a://garcia-capstone/input/', 's3a://garcia-capstone/output/']
        }
    },
  
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Main_ETL'
}


start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
)    
    
add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=CAP_EMR_Steps,
    dag=dag
    )        
        
check_main_processing = EmrStepSensor(
    task_id='MAIN_ETL_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

    
delete_cluster = EmrTerminateJobFlowOperator(
    task_id='Delete_EMR_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)


#Create Tables for Redshift
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="CAP_Create_tables2.sql",
)


accident_to_redshift = StageToRedshiftOperator(
    task_id='accidents_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='accident',
    s3_bucket='garcia-capstone/output',
    s3_key='accident',
    iam_cred= 'aws_iam_role=arn:aws:iam::944264041230:role/dwhrole',
    #region='us-west-2',
    _format =  'PARQUET',
    #delimiter= ',',
    #extra_params= 'IGNOREHEADER 1',
)

weather_to_redshift = StageToRedshiftOperator(
    task_id='weather_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='weather',
    s3_bucket='garcia-capstone/output',
    s3_key='weather',
    iam_cred= 'aws_iam_role=arn:aws:iam::944264041230:role/dwhrole',
    #region='us-west-2',
    _format = 'PARQUET',
    #delimiter= ',',
    #extra_params= 'IGNOREHEADER 1',
)

location_to_redshift = StageToRedshiftOperator(
    task_id='location_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='location',
    s3_bucket='garcia-capstone/output',
    s3_key='location',
    iam_cred= 'aws_iam_role=arn:aws:iam::944264041230:role/dwhrole',
    #region='us-west-2',
    _format = 'PARQUET',
    #delimiter= ',',
    #extra_params= 'IGNOREHEADER 1',
)

time_to_redshift = StageToRedshiftOperator(
    task_id='time_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='time',
    s3_bucket='garcia-capstone/output',
    s3_key='time',
    iam_cred= 'aws_iam_role=arn:aws:iam::944264041230:role/dwhrole',
    #region='us-west-2',
    _format = 'PARQUET',
    #delimiter= ',',
    #extra_params= 'IGNOREHEADER 1',
)

area_to_redshift = StageToRedshiftOperator(
    task_id='area_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='area_poi',
    s3_bucket='garcia-capstone/output',
    s3_key='area_poi',
    iam_cred= 'aws_iam_role=arn:aws:iam::944264041230:role/dwhrole',
    #region='us-west-2',
    _format = 'PARQUET',
    #delimiter= ',',
    #extra_params= 'IGNOREHEADER 1',
)

accident_quality_checks = DataQualityOperator(
    task_id='accident_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='accident',
)
weather_quality_checks = DataQualityOperator(
    task_id='weather_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='weather',
)
location_quality_checks = DataQualityOperator(
    task_id='location_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='location',
)
time_quality_checks = DataQualityOperator(
    task_id='time_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
)

area_quality_checks = DataQualityOperator(
    task_id='area_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table='area_poi',
)
end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator                                        >> create_cluster 

create_cluster                                        >> add_jobflow_steps
add_jobflow_steps                               >> check_main_processing
check_main_processing                     >> delete_cluster
check_main_processing                     >> create_tables_task

[delete_cluster, create_tables_task] >> accident_to_redshift
[delete_cluster, create_tables_task] >> weather_to_redshift
[delete_cluster, create_tables_task] >> location_to_redshift
[delete_cluster, create_tables_task] >> time_to_redshift
[delete_cluster, create_tables_task] >> area_to_redshift

accident_to_redshift                             >> accident_quality_checks
weather_to_redshift                              >> weather_quality_checks
location_to_redshift                              >> location_quality_checks
time_to_redshift                                     >> time_quality_checks
area_to_redshift                                     >> area_quality_checks

accident_quality_checks                     >> end_operator
weather_quality_checks                      >> end_operator
location_quality_checks                      >> end_operator
time_quality_checks                             >> end_operator
area_quality_checks                             >> end_operator