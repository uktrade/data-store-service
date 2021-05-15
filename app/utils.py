import json

import requests
from flask import current_app as flask_app
from mohawk import Sender


def hawk_api_request(
    url,
    method,
    credentials,
    body=None,
):
    if body:
        body = json.dumps(body)
    auth_header = Sender(
        credentials,
        url,
        method.lower(),
        content_type="application/json" if body else None,
        content=body,
    ).request_header

    headers = {"Authorization": auth_header}
    if body:
        headers["Content-Type"] = "application/json"
    response = requests.request(
        method,
        url,
        data=body,
        headers=headers,
    )

    response.raise_for_status()
    response_json = response.json()
    return response_json


def trigger_dataflow_dag(schema, table):
    base_url, dag_id, dataflow_hawk_creds = _get_dataflow_api_params()
    dataflow_source_url = f"{base_url}/api/experimental/dags/{dag_id}/dag_runs"
    dag_config = {'conf': {'data_uploader_schema_name': schema, 'data_uploader_table_name': table}}
    return hawk_api_request(dataflow_source_url, "POST", dataflow_hawk_creds, dag_config)


def check_dataflow_dag_progress(run_id):
    base_url, dag_id, dataflow_hawk_creds = _get_dataflow_api_params()
    dataflow_source_url = f"{base_url}/api/experimental/dags/{dag_id}/dag_runs"
    return hawk_api_request(
        f'{dataflow_source_url}/{run_id}',
        "GET",
        dataflow_hawk_creds,
    )


def _get_dataflow_api_params():
    dag_id = flask_app.config['dataflow']['data_uploader_dag_id']
    base_url = flask_app.config['dataflow']['base_url']
    dataflow_hawk_creds = {
        "id": flask_app.config['dataflow']['hawk_id'],
        "key": flask_app.config['dataflow']['hawk_key'],
        "algorithm": "sha256",
    }
    return base_url, dag_id, dataflow_hawk_creds
