import datetime
import logging
import os.path
import time
from threading import Thread

from datatools.io.fileinfo import FileInfo
from datatools.io.storage import StorageFactory
from flask import copy_current_request_context, current_app as app
from sqlalchemy.sql.functions import now

from app.constants import DataUploaderFileState
from app.db.models.internal import PipelineDataFile
from app.etl.pipeline_type.dsv_to_table import DSVToTablePipeline
from app.uploader.csv_parser import CSVParser
from app.utils import check_dataflow_dag_progress


def upload_file(file_name, stream):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    storage.write_file(file_name, stream)


def delete_file(pipeline_data_file):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    storage.delete_file(pipeline_data_file.data_file_url)


def _move_file_to_s3(file_url, organisation, dataset, delimiter, quote):
    bucket = app.config['s3']['bucket_url']
    file_name = file_url.split('/')[-1]
    full_url = os.path.join(bucket, file_url)
    utf_8_byte_stream = CSVParser.get_csv_as_utf_8_byte_stream(
        full_url=full_url, delimiter=delimiter, quotechar=quote,
    )
    file_info = FileInfo(file_url, utf_8_byte_stream)
    storage = StorageFactory.create(bucket)
    datasets_folder = app.config['s3']['datasets_folder']
    target_file_url = f'{datasets_folder}/{organisation}/{dataset}/{file_name}'
    storage.write_file(target_file_url, file_info.data)
    file_info.data.seek(0)
    return file_info


def check_for_reserved_column_names(pipeline_data_file, uploaded_columns):
    pipeline = pipeline_data_file.pipeline

    dsv_pipeline = DSVToTablePipeline(
        dbi=app.dbi,
        organisation=pipeline.organisation,
        dataset=pipeline.dataset,
        data_column_types=[],
        quote=pipeline_data_file.quote,
        separator=pipeline_data_file.delimiter,
    )

    reserved_l0_columns = set(x[0] for x in dsv_pipeline.l0_helper_columns)
    reserved_l1_columns = set(x[0] for x in dsv_pipeline.l1_helper_columns)
    reserved_columns = reserved_l0_columns.union(reserved_l1_columns)

    conflicting_columns = uploaded_columns.intersection(reserved_columns)
    if conflicting_columns:
        joined_columns = ', '.join(map(lambda x: f'“{x}”', conflicting_columns))
        error_message = f"Unable to process CSV file: {joined_columns} "

        if len(conflicting_columns) > 1:
            error_message += (
                "in the uploaded file are reserved column names. "
                "You must rename those columns in the data file."
            )
        else:
            error_message += (
                "in the uploaded file is a reserved column name. "
                "You must rename that column in the data file."
            )
        return error_message

    return None


def process_pipeline_data_file(pipeline_data_file):
    pipeline = pipeline_data_file.pipeline
    organisation = pipeline.organisation
    dataset = pipeline.dataset
    column_types = pipeline_data_file.column_types
    delimiter = pipeline_data_file.delimiter
    quote = pipeline_data_file.quote

    pipeline_data_file.started_processing_at = now()
    pipeline_data_file.save()

    # move file to s3
    file_url = pipeline_data_file.data_file_url
    file_info = _move_file_to_s3(file_url, organisation, dataset, delimiter, quote,)

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
        data_column_types=column_types,
        quote=quote,
        separator=delimiter,
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
            pipeline_data_file.error_message = str(e).split('CONTEXT')[0]
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
        pipeline, pipeline_data_file,
    ):
        try:
            pipeline_data_file.state = DataUploaderFileState.PROCESSING_DATAFLOW.value
            pipeline_data_file.save()
            response = pipeline.trigger_dataflow_dag()
            run_id = response['run_id'].split('__')[1].split('+')[0]
            state = 'running'
            count = 0
            while state == 'running':
                if count > 360:  # stop waiting after 30 mins
                    raise Exception("We can't wait forever for airflow task completion!")
                time.sleep(5)
                response = check_dataflow_dag_progress(run_id)
                state = response['state']
                count += 1
            if state == 'success':
                pipeline_data_file.state = DataUploaderFileState.COMPLETED.value
                pipeline_data_file.processed_at = now()
            else:
                raise Exception("Airflow dag failed")
            pipeline_data_file.save()
        except Exception as e:
            pipeline_data_file.error_message = str(e)
            pipeline_data_file.state = DataUploaderFileState.FAILED.value
            pipeline_data_file.save()
            raise

    _trigger_dag_params = {
        'pipeline': dsv_pipeline,
        'pipeline_data_file': pipeline_data_file,
    }

    thread = PipelineThread(
        process_pipeline=(_process_pipeline, _process_pipeline_params),
        trigger_dag=(_trigger_dag, _trigger_dag_params),
    )
    return thread


def mark_old_processing_data_files_as_failed(app):
    one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

    logging.info("Checking for procesing pipelines running for more than 24 hours.")
    with app.app_context():
        for pipeline_data_file in PipelineDataFile.query.filter(
            PipelineDataFile.state.in_(
                (
                    DataUploaderFileState.VERIFIED.value,
                    DataUploaderFileState.PROCESSING_DSS.value,
                    DataUploaderFileState.PROCESSING_DATAFLOW.value,
                )
            )
        ):
            if (
                pipeline_data_file.started_processing_at is None
                or pipeline_data_file.started_processing_at <= one_day_ago
            ):
                logging.info(
                    "Marking %s as failed (didn't complete within 24 hours)", pipeline_data_file
                )
                pipeline_data_file.state = DataUploaderFileState.FAILED.value
                pipeline_data_file.error_message = (
                    "Failed for an unknown reason. Pipeline was automatically marked as failed "
                    "because it did not complete within 24 hours."
                )
                pipeline_data_file.save()
