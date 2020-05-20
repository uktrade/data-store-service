from datatools.io.storage import StorageFactory
from flask import current_app as app


def upload_file(stream, file_name, pipeline):
    storage = StorageFactory.create(app.config['inputs']['source-folder'])
    file_name = f'{pipeline.organisation}/{pipeline.dataset}/{file_name}'
    abs_fn = storage._abs_file_name(file_name)
    s3 = storage._get_bucket()
    s3.upload_fileobj(stream, abs_fn)
