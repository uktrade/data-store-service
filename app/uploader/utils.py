import csv
import json
import os.path
import re
import time
from io import BytesIO
from threading import Thread

import pandas
import pandas as pd
import requests
from datatools.io.fileinfo import FileInfo
from datatools.io.storage import StorageFactory
from flask import copy_current_request_context, current_app as app
from mohawk import Sender
from smart_open import open
from sqlalchemy.sql.functions import now

from app.constants import DataUploaderFileState
from app.etl.pipeline_type.dsv_to_table import DSVToTablePipeline


def upload_file(file_name, stream):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    storage.write_file(file_name, stream)


def delete_file(pipeline_data_file):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    storage.delete_file(pipeline_data_file.data_file_url)


def get_s3_file_sample(url, delimiter, quotechar, number_of_lines=4):
    bucket = app.config['s3']['bucket_url']
    full_url = os.path.join(bucket, url)
    try:
        with open(full_url, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file, quotechar=quotechar, delimiter=delimiter,)
            csv_headings = next(csv_reader)
            if len(csv_headings) != len(set(csv_headings)):
                raise csv.Error('Unable to process CSV file: duplicate header names not allowed')
            if '' in csv_headings:
                raise csv.Error('Unable to process CSV file: empty header names not allowed')

            invalid_headings = list(
                filter(lambda x: not re.match("^[a-z][a-z0-9_]+$", x), csv_headings)
            )
            if invalid_headings:
                joined_invalid_headings = '"' + '", "'.join(invalid_headings) + '"'
                raise csv.Error(
                    f"Unable to process CSV file: column headers must start with a letter and "
                    f"may only contain lowercase letters, numbers, and underscores. Invalid "
                    f"headers: {joined_invalid_headings}"
                )

            csv_contents = []
            count = 0
            for row in csv_reader:
                if count == number_of_lines:
                    break
                if len(row) != len(csv_headings):
                    raise csv.Error(
                        f'Unable to process CSV file: some rows have a different number of data '
                        f'points ({len(row)}) than there are column headers ({len(csv_headings)})'
                    )
                csv_contents.append(row)
                count += 1

        df = pd.DataFrame(csv_contents, columns=csv_headings)
        return df, None
    except (UnicodeDecodeError, csv.Error) as e:
        if isinstance(e, UnicodeDecodeError):
            error_message = f'The CSV file could not be opened. (Technical details: {str(e)})'
        else:
            error_message = str(e)

        return pd.DataFrame(), error_message


def _move_file_to_s3(file_url, organisation, dataset):
    bucket = app.config['s3']['bucket_url']
    file_name = file_url.split('/')[-1]
    full_url = os.path.join(bucket, file_url)
    file_contents = open(full_url, encoding='utf-8-sig').read()
    file_contents_utf_8 = BytesIO(str.encode(file_contents, 'utf-8'))
    file_contents_utf_8.seek(0)
    file_info = FileInfo(file_url, file_contents_utf_8)

    storage = StorageFactory.create(bucket)
    datasets_folder = app.config['s3']['datasets_folder']
    target_file_url = f'{datasets_folder}/{organisation}/{dataset}/{file_name}'
    storage.write_file(target_file_url, file_info.data)
    file_info.data.seek(0)
    return file_info


def process_pipeline_data_file(pipeline_data_file):
    pipeline = pipeline_data_file.pipeline
    organisation = pipeline.organisation
    dataset = pipeline.dataset

    # move file to s3
    file_url = pipeline_data_file.data_file_url
    file_info = _move_file_to_s3(file_url, organisation, dataset)

    # create pipeline
    class PipelineThread(Thread):
        def __init__(
            self, process_pipeline, trigger_dag,
        ):
            self.process_pipeline = process_pipeline
            self.trigger_dag = trigger_dag
            super().__init__()

        def run(self):
            self.process_pipeline[0](**self.process_pipeline[1])
            self.trigger_dag[0](**self.trigger_dag[1])

    dsv_pipeline = DSVToTablePipeline(
        dbi=app.dbi,
        organisation=organisation,
        dataset=dataset,
        data_column_types=pipeline.column_types,
        quote=pipeline.quote,
        separator=pipeline.delimiter,
    )

    @copy_current_request_context
    def _process_pipeline(
        pipeline, pipeline_data_file, file_info,
    ):
        try:
            pipeline_data_file = app.db.session.merge(pipeline_data_file)
            pipeline_data_file.state = DataUploaderFileState.PROCESSING_DSS.value
            pipeline_data_file.save()
            pipeline.process(file_info)
            time.sleep(5)
        except Exception as e:
            pipeline_data_file.error_message = str(e)
            pipeline_data_file.state = DataUploaderFileState.FAILED.value
            pipeline_data_file.save()
            raise

    _process_pipeline_params = {
        'pipeline': dsv_pipeline,
        'pipeline_data_file': pipeline_data_file,
        'file_info': file_info,
    }

    @copy_current_request_context
    def _trigger_dag(
        dataflow_source_url, dag_config, dataflow_hawk_creds, pipeline_data_file,
    ):
        try:
            pipeline_data_file.state = DataUploaderFileState.PROCESSING_DATAFLOW.value
            pipeline_data_file.save()
            response = hawk_api_request(
                dataflow_source_url, "POST", dataflow_hawk_creds, dag_config
            )
            run_id = response['run_id'].split('__')[1].split('+')[0]
            state = 'running'
            count = 0
            while state == 'running':
                if count > 360:  # stop waiting after 30 mins
                    raise Exception("We can't wait forever for airflow task completion!")
                time.sleep(5)
                response = hawk_api_request(
                    f'{dataflow_source_url}/{run_id}', "GET", dataflow_hawk_creds,
                )
                state = response['state']
                count += 1
            if state == 'success':
                pipeline_data_file.state = DataUploaderFileState.COMPLETED.value
                pipeline_data_file.processed_at = now()
            else:
                pipeline_data_file.error_message = 'Airflow dag failed'
                pipeline_data_file.state = DataUploaderFileState.FAILED.value
            pipeline_data_file.save()
        except Exception as e:
            pipeline_data_file.error_message = str(e)
            pipeline_data_file.state = DataUploaderFileState.FAILED.value
            pipeline_data_file.save()
            raise

    dag_id = app.config['dataflow']['data_uploader_dag_id']
    base_url = app.config['dataflow']['base_url']
    dataflow_source_url = f"{base_url}/api/experimental/dags/{dag_id}/dag_runs"
    dag_config = {
        'conf': {
            'data_uploader_schema_name': dsv_pipeline.schema,
            'data_uploader_table_name': dsv_pipeline.L0_TABLE,
        }
    }
    dataflow_hawk_creds = {
        "id": app.config['dataflow']['hawk_id'],
        "key": app.config['dataflow']['hawk_key'],
        "algorithm": "sha256",
    }
    _trigger_dag_params = {
        'pipeline_data_file': pipeline_data_file,
        'dataflow_source_url': dataflow_source_url,
        'dag_config': dag_config,
        'dataflow_hawk_creds': dataflow_hawk_creds,
    }

    thread = PipelineThread(
        process_pipeline=(_process_pipeline, _process_pipeline_params),
        trigger_dag=(_trigger_dag, _trigger_dag_params),
    )
    return thread


def hawk_api_request(
    url, method, credentials, body=None,
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
    response = requests.request(method, url, data=body, headers=headers,)

    response.raise_for_status()
    response_json = response.json()
    return response_json


def get_column_types(file_contents):
    columns = file_contents.columns.to_list()
    return list(zip(columns, ['text'] * len(columns)))
