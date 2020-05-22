import csv
import datetime
import os.path

import pandas
from datatools.io.storage import StorageFactory
from flask import current_app as app
from smart_open import open


def upload_file(stream, pipeline):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    upload_folder = app.config['s3']['upload_folder']
    file_name = f'{upload_folder}/{pipeline.file_name}'
    obj = storage.write_file(file_name, stream)
    # return file_name, obj.version_id
    import random
    return file_name, random.randint(0, 1000)


def delete_file(pipeline):
    storage = StorageFactory.create(app.config['s3']['bucket_url'])
    upload_folder = app.config['s3']['upload_folder']
    file_name = f'{upload_folder}/{pipeline.file_name}'
    storage.delete_file(file_name)


def get_versions(pipeline_data_file):
    return [
        {"number": 1, "uploaded_at": datetime.datetime.now(), "latest": True},
        {"number": 2, "uploaded_at": datetime.datetime.now(), "latest": False},
        {"number": 3, "uploaded_at": datetime.datetime.now(), "latest": False},
    ]


def get_s3_file_sample(url, delimiter, quotechar, number_of_lines=4, version=None):
    bucket = app.config['s3']['bucket_url']
    full_url = os.path.join(bucket, url)
    contents = []
    i = 0
    transport_params = {}
    # if version:
    #     transport_params = {'version_id': 'version'}
    try:
        with open(full_url, encoding='utf-8-sig', transport_params=transport_params) as csv_file:
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
