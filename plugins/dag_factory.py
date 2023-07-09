from airflow import DAG
from airflow.utils.module_loading import import_string
from datetime import datetime
from importlib import import_module
import yaml


def read_config(config_filepath):
    return yaml.load(
        stream=open(config_filepath, "r", encoding="utf-8"),
        Loader=yaml.FullLoader,
    )

def import_modules(module_path):
    module_name, class_name = module_path.rsplit(".", 1)
    module = import_module(module_name)
    callable = getattr(module, class_name)

    return callable

def create_task(task_config, task_mapper, dag):
    task_id = task_config['task_id']
    operator_path = task_config['operator']
    params = task_config['params']
    upstream_tasks = task_config.get('upstream', [])

    operator_class = import_modules(operator_path)

    callable = params.get('python_callable')
    if callable:
      params['python_callable'] = import_string(callable)

    task = operator_class(
        task_id=task_id,
        dag=dag,
        **params
    )

    for upstream_task_id in upstream_tasks:
        upstream_task = task_mapper.get(upstream_task_id)
        if upstream_task:
            task.set_upstream(upstream_task)

    return task

def create_dag(config_filepath, globals) -> DAG:
    cfg = read_config(config_filepath)

    dag_id = cfg['airflow']['dag_id']
    schedule_interval = cfg['airflow']['schedule_interval']

    year, month, date = list(map(int, cfg['airflow']['start_date'].split('-')))

    default_args = {
        "owner": cfg['airflow']['dag_id'],
        "start_date": datetime(year, month, date)
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    task_mapper = {}

    for task_config in cfg['tasks']:
        task = create_task(task_config, task_mapper, dag)
        task_mapper[task.task_id] = task

    globals[dag.dag_id] = dag

    return dag
