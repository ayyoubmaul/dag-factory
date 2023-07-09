from dag_factory import create_dag
import os

cfg_dir = '/opt/airflow/configs'

for file in os.listdir(cfg_dir):
    create_dag(os.path.join(cfg_dir, file), globals())
