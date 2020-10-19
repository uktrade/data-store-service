from datetime import datetime, timedelta
from unittest import mock

from datatools.io.storage import S3Storage

from app.constants import DataUploaderFileState
from app.db.models.internal import PipelineDataFile
from app.uploader.utils import mark_old_processing_data_files_as_failed, upload_file
from tests.fixtures.factories import PipelineDataFileFactory


@mock.patch('app.uploader.utils.StorageFactory.create')
def test_upload_file(mock_create_storage, app_with_db):
    with mock.patch.object(S3Storage, 'write_file', return_value=None) as mock_write_file:
        mock_create_storage.return_value = S3Storage('s3://bucket')
        upload_file('test.csv', 'bla')
    assert mock_create_storage.called is True
    assert mock_write_file.called is True


def test_mark_old_processing_data_files_as_failed(app_with_db):
    recent_uuid = 'ed67ecea-9567-4f87-a5b5-eb517d84ce6c'
    PipelineDataFileFactory(
        id=recent_uuid,
        state=DataUploaderFileState.VERIFIED.value,
        started_processing_at=datetime.now() - timedelta(hours=1),
    )
    old_uuid = '7849ab8c-96e4-40ff-afc7-40ae1decd6b2'
    PipelineDataFileFactory(
        id=old_uuid,
        state=DataUploaderFileState.VERIFIED.value,
        started_processing_at=datetime.now() - timedelta(days=2),
    )

    mark_old_processing_data_files_as_failed(app_with_db)

    assert PipelineDataFile.query.get(recent_uuid).state == DataUploaderFileState.VERIFIED.value
    assert PipelineDataFile.query.get(old_uuid).state == DataUploaderFileState.FAILED.value
