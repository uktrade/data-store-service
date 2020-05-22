import csv
import os.path

import pandas
from datatools.io.storage import StorageFactory
from flask import current_app as app
from smart_open import open

import boto3

from boto3.s3.transfer import TransferConfig

MB = 100

config = TransferConfig (**{
    'multipart_threshold': 1024 * MB,
    'max_concurrency': 5,
    'multipart_chunksize': 1024 * MB,
    'use_threads': True
})


class UploadFile:

    def __init__(self):
        self.total = 0
        self.uploaded = 0
        self.s3 = boto3.client('s3')

    def upload_callback(self, size):
        if self.total == 0:
            return
        self.uploaded += size
        percentage = int(self.uploaded / self.total * 100)
        print(f'{percentage} % - ({size}) - Total {self.total} - Uploaded {self.uploaded}')

    def upload(self, stream, file_name, pipeline):
        self.total = get_size(stream)
        bucket = app.config['s3']['bucket_url']
        bucket = bucket.replace('s3://', '')
        upload_folder = app.config['s3']['upload_folder']
        file_name = f'{upload_folder}/{pipeline.organisation}/{pipeline.dataset}/{file_name}'
        self.s3.upload_fileobj(
            stream, bucket, file_name, Config=config, Callback=self.upload_callback
        )
        return file_name


def get_size(fobj):
    if fobj.content_length:
        return fobj.content_length

    try:
        pos = fobj.tell()
        fobj.seek(0, 2)  #seek to end
        size = fobj.tell()
        fobj.seek(pos)  # back to original position
        return size
    except (AttributeError, IOError):
        pass

    # in-memory file object that doesn't support seeking or tell
    return 0  #assume small enough


def upload_file(stream, file_name, pipeline):
    u = UploadFile()
    file_name = u.upload(stream, file_name, pipeline)
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
    move_file_in_s3(pipeline_data_file)


def move_file_in_s3(pipeline_data_file):
    pass


def save_column_types(pipeline, file_contents):
    columns = file_contents.columns.to_list()
    mapped_column_types = list(zip(columns, ['text'] * len(columns)))
    pipeline.column_types = mapped_column_types
    pipeline.save()
