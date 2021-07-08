from datetime import datetime
from biz_gp_dag import load_subdag as gp
from biz_darlehen_dag import load_subdag as darlehen
from biz_konto_dag import load_subdag as konto
from biz_kreditkarte_dag import load_subdag as karte
from biz_transaktion_dag import load_subdag as trans
from biz_l_cc_konto_dag import load_subdag as cc_konto
from biz_l_gp_konto_dag import load_subdag as gp_konto
from biz_l_darlehen_konto_dag import load_subdag as darlehen_konto
from biz_l_trans_konto_dag import load_subdag as trans_konto
from biz_l_gp_cc_dag import load_subdag as gp_cc
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.external_task import ExternalTaskSensor

DAG_NAME = "BIZ_loading"

default_args={
        "owner":"airflow",
        'start_date': datetime(2021,6,12)
    }

with DAG(
    dag_id=DAG_NAME, default_args=default_args,start_date= datetime(2021, 7, 8), schedule_interval="@once", tags=['ENB']
) as dag:

    wait_for_lookups = ExternalTaskSensor(
        task_id='wait_fr_lookups',
        external_dag_id='load_lookups',
        external_task_id='end'
    )

    start = DummyOperator(
        task_id='start',
    )

    gp = SubDagOperator(
        task_id='load_gp_biz',
        subdag=gp(parent_dag_name=DAG_NAME, child_dag_name="load_gp_biz",args=default_args),
        default_args=default_args,
        dag=dag
    )

    dar = SubDagOperator(
        task_id='load_darlehen_biz',
        subdag=darlehen(parent_dag_name=DAG_NAME, child_dag_name="load_darlehen_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    konto = SubDagOperator(
        task_id='load_konto_biz',
        subdag=konto(parent_dag_name=DAG_NAME, child_dag_name="load_konto_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    karte = SubDagOperator(
        task_id='load_kreditkarte_biz',
        subdag=karte(parent_dag_name=DAG_NAME, child_dag_name="load_kreditkarte_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    trans = SubDagOperator(
        task_id='load_transaktion_biz',
        subdag=trans(parent_dag_name=DAG_NAME, child_dag_name="load_transaktion_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    cc_konto = SubDagOperator(
        task_id='load_l_cc_konto_biz',
        subdag=cc_konto(parent_dag_name=DAG_NAME, child_dag_name="load_l_cc_konto_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    darlehen_konto = SubDagOperator(
        task_id='load_l_darlehen_konto_biz',
        subdag=darlehen_konto(parent_dag_name=DAG_NAME, child_dag_name="load_l_darlehen_konto_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    gp_konto = SubDagOperator(
        task_id='load_l_gp_konto_biz',
        subdag=gp_konto(parent_dag_name=DAG_NAME, child_dag_name="load_l_gp_konto_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    trans_konto = SubDagOperator(
        task_id='load_l_trans_konto_biz',
        subdag=trans_konto(parent_dag_name=DAG_NAME, child_dag_name="load_l_trans_konto_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    gp_cc = SubDagOperator(
        task_id='load_l_gp_cc_biz',
        subdag=gp_cc(parent_dag_name=DAG_NAME, child_dag_name="load_l_gp_cc_biz", args=default_args),
        default_args=default_args,
        dag=dag
    )

    entity = DummyOperator(
        task_id='entity_finished',
    )

    end = DummyOperator(
        task_id='end',
    )

wait_for_lookups >> start >> [gp,dar,konto,karte,trans] >> entity
entity >> [cc_konto,darlehen_konto,trans_konto,gp_konto,gp_cc] >> end