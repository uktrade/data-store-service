from unittest import mock

from datatools.io.storage import S3Storage

from app.uploader.utils import upload_file


@mock.patch('app.uploader.utils.StorageFactory.create')
def test_upload_file(mock_create_storage, app_with_db):
    with mock.patch.object(S3Storage, 'write_file', return_value=None) as mock_write_file:
        mock_create_storage.return_value = S3Storage('s3://bucket')
        upload_file('test.csv', 'bla')
    assert mock_create_storage.called is True
    assert mock_write_file.called is True
