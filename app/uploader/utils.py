import csv
import os.path
from io import BytesIO
from threading import Thread

import pandas
from datatools.io.fileinfo import FileInfo
from datatools.io.storage import StorageFactory
from flask import copy_current_request_context, current_app as app
from smart_open import open
from sqlalchemy.sql.functions import now

from app.etl.pipeline_type.dsv_to_table import DSVToTablePipeline


def upload_file(stream, file_name, pipeline):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    upload_folder = app.config['s3']['upload_folder']
    file_name = f'{upload_folder}/{pipeline.organisation}/{pipeline.dataset}/{file_name}'
    storage.write_file(file_name, stream)
    return file_name


def get_s3_file_sample(url, delimiter, quotechar, number_of_lines=4):
    bucket = app.config['s3']['bucket_url']
    full_url = os.path.join(bucket, url)
    contents = []
    i = 0
    try:
        with open(full_url, encoding='utf-8-sig') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=delimiter, quotechar=quotechar, strict=True)
            for row in reader:
                contents.append(row)
                i += 1
                if i == number_of_lines:
                    break

        df = pandas.DataFrame(data=contents)
        empty_column = None in df.columns.to_list()
        if empty_column:
            raise csv.Error('Invalid CSV')
        return df
    except (UnicodeDecodeError, csv.Error):
        return pandas.DataFrame()


def process_pipeline_data_file(pipeline_data_file):
    bucket = app.config['s3']['bucket_url']
    file_url = pipeline_data_file.data_file_url
    file_name = file_url.split('/')[-1]
    full_url = os.path.join(bucket, file_url)
    file_contents = open(full_url, encoding='utf-8-sig').read()
    file_contents_utf_8 = BytesIO(str.encode(file_contents, 'utf-8'))
    file_contents_utf_8.seek(0)
    file_info = FileInfo(file_url, file_contents_utf_8)

    # move file to s3 dataset folder
    storage = StorageFactory.create(bucket)
    pipeline = pipeline_data_file.pipeline
    organisation = pipeline.organisation
    dataset = pipeline.dataset
    datasets_folder = app.config['s3']['datasets_folder']
    target_file_url = f'{datasets_folder}/{organisation}/{dataset}/{file_name}'
    storage.write_file(target_file_url, file_info.data)
    file_info.data.seek(0)

    # create pipeline
    class PipelineThread(Thread):
        def __init__(
            self, pipeline, pipeline_data_file, file_info, ctx_bridge,
        ):
            self.pipeline = pipeline
            self.pipeline_data_file = pipeline_data_file
            self.file_info = file_info
            self.ctx_bridge = ctx_bridge
            super().__init__()

        def run(self):
            self.file_info.data.seek(0)
            self.ctx_bridge(self.pipeline, self.pipeline_data_file, self.file_info)

    dsv_pipeline = DSVToTablePipeline(
        dbi=app.dbi,
        organisation=organisation,
        dataset=dataset,
        data_column_types=pipeline.column_types,
        quote=pipeline.quote,
        separator=pipeline.delimiter,
    )

    @copy_current_request_context
    def ctx_bridge(pipeline, pipeline_data_file, file_info):
        pipeline.process(file_info)
        pipeline_data_file.processed_at = now()
        pipeline_data_file.save()

    thread = PipelineThread(
        pipeline=dsv_pipeline,
        pipeline_data_file=pipeline_data_file,
        file_info=file_info,
        ctx_bridge=ctx_bridge,
    )
    return thread


def save_column_types(pipeline, file_contents):
    columns = file_contents.columns.to_list()
    mapped_column_types = list(zip(columns, ['text'] * len(columns)))
    pipeline.column_types = mapped_column_types
    pipeline.save()
