import json
import pandas as pd


def extract_data(file):
    return pd.read_csv(file).to_dict('record')

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')

    return [value for value in data if value[kwargs['filter']] == kwargs['value']]

def load_data(output, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')

    f = open(output, 'w')
    f.write(json.dumps(data))
