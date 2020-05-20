import csv

import pandas
from datatools.io.storage import StorageFactory
from flask import current_app as app
from smart_open import open


def upload_file(stream, file_name, pipeline):
    storage = StorageFactory.create(app.config['inputs']['upload-folder'])
    file_name = f'{pipeline.organisation}/{pipeline.dataset}/{file_name}'
    abs_fn = storage._abs_file_name(file_name)
    s3 = storage._get_bucket()
    s3.upload_fileobj(stream, abs_fn)
    return file_name


def get_s3_file_sample(url, delimiter, quotechar, number_of_lines=4):
    FOLDER = app.config['inputs']['upload-folder']
    full_url = f'{FOLDER}/{url}'
    contents = []
    i = 0
    with open(full_url, encoding='utf-8-sig') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=delimiter, quotechar=quotechar, strict=True)
        for row in reader:
            contents.append(row)
            i += 1
            if i == number_of_lines:
                break

    df = pandas.DataFrame(data=contents)
    return df


def process_pipeline_data_file(pipeline_data_file):
    move_file_in_s3(pipeline_data_file)


def move_file_in_s3(pipeline_data_file):
    pass


def save_column_types(pipeline, file_contents):
    columns = file_contents.columns.to_list()
    mapped_column_types = list(zip(columns, ['text'] * len(columns)))
    pipeline.column_types = mapped_column_types
    pipeline.save()
