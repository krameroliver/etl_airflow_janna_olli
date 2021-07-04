# from airflow.operators.subdag_operator import  SubDagOperator
# from airflow import DAG
# from datetime import timedelta, datetime
#
# from airflow.sensors.external_task import ExternalTaskSensor
#
# from biz_gp_dag import subDabGP
#
# default_args={
#         "owner":"airflow",
#         'start_date': datetime(2021,6,12)
#     }
#
# d = DAG(
# schedule_interval="@daily",
#         default_args=default_args,
#         catchup=False
#
# )
#
# xternalsensor1 = ExternalTaskSensor(
#             task_id='src_dag_completed_status',
#             external_dag_id='load_all_source',
#             external_task_id='end',  # wait for whole DAG to complete
#             check_existence=True,
#             start_date=datetime(2021,6,12),
#             timeout=120)
#
# gp_biz = SubDagOperator(dag=subDabGP,
#                         default_args=default_args
#                         )