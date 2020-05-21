import csv
import os.path

import pandas
from datatools.io.storage import StorageFactory
from flask import current_app as app
from smart_open import open


def upload_file(stream, file_name, pipeline):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    upload_folder = app.config['s3']['upload_folder']
    file_name = f'{upload_folder}/{pipeline.organisation}/{pipeline.dataset}/{file_name}'
    abs_fn = storage._abs_file_name(file_name)
    s3 = storage._get_bucket()
    _upload_file_stream(s3, stream, abs_fn)
    return file_name


def _upload_file_stream(s3, stream, abs_fn):
    s3.upload_fileobj(stream, abs_fn)


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
    move_file_in_s3(pipeline_data_file)


def move_file_in_s3(pipeline_data_file):
    pass


def save_column_types(pipeline, file_contents):
    columns = file_contents.columns.to_list()
    mapped_column_types = list(zip(columns, ['text'] * len(columns)))
    pipeline.column_types = mapped_column_types
    pipeline.save()
