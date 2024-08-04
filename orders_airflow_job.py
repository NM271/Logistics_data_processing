from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (DataprocSubmitPySparkJobOperator,DataprocCreateClusterOperator,
                                                               DataprocDeleteClusterOperator)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable


default_args={
    'owner':'airflow',
    'depends_on_past':True,
    'email_on_filure':False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_logistics_orders',
    default_args=default_args,
    description='logistics data processing job',
    schedule_interval=None,
    start_date=datetime(2024, 6, 6),
    tags=['procss_orders_data'],
)

CLUSTER_NAME = 'airflow-spark-job'
PROJECT_ID = 'aerobic-datum-424113-v0'
REGION = 'us-east1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.1-debian11'  # Image version
    }
}



# crate sensor object to scan the bucket for specific interval of time
sensor = GCSObjectExistenceSensor(
    task_id= 'sensor_operator',
    bucket= "logistics_orders_bucket",
    object= "input_data/orders.csv",
    poke_interval= 300,
    mode='poke',
    timeout= 12*60*60,
    dag=dag,
)

create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
    )

pyspark_job={"main_python_file_uri":"gs://logistics_orders_bucket/spark_script/logistics_spark_job.py"}
submit_pyspark_job=DataprocSubmitPySparkJobOperator(
            task_id='pyspark_job',         
            main=pyspark_job["main_python_file_uri"],
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            region=REGION,
            # dataproc_pyspark_properties=spark_job_resource_parm,
            dag=dag,
        )
# # ensures deleting cluster 
# delete_cluster = DataprocDeleteClusterOperator(
#     task_id='delete_cluster',
#     cluster_name=CLUSTER_NAME,
#     project_id=PROJECT_ID,
#     region=REGION,
#     trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
#     dag=dag,
# )

sensor >> create_cluster >> submit_pyspark_job 