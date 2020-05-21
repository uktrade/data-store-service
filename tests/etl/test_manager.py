import os.path
from datetime import datetime
from unittest import mock

import pytest
from freezegun import freeze_time

from app.constants import DatafileState
from app.db.models.internal import DatafileRegistryModel
from app.etl.manager import DSSDatafileProvider, Manager, PipelineConfig


class FakePipeline:
    def __init__(self, _id, raise_processing_exception=None):
        self.id = str(_id)
        self.raise_processing_exception = raise_processing_exception

    def process(self, *args, **kwargs):
        if self.raise_processing_exception:
            raise Exception(self.raise_processing_exception)


class TestETLManager:
    @pytest.mark.parametrize(
        'pipeline,expected_result', (('Hello', 'Hello'), (FakePipeline(2), '2'),),
    )
    def test_to_pipeline_id(self, pipeline, expected_result):
        manager = Manager()
        assert manager._to_pipeline_id(pipeline) == expected_result

    @pytest.mark.parametrize(
        'pipeline,expected_result', (('Hello', 'Hello'), (FakePipeline(2), '2'),),
    )
    def test_pipeline_get(self, pipeline, expected_result):
        manager = Manager()
        fake_pipeline = FakePipeline(0)
        manager._pipelines[getattr(pipeline, 'id', pipeline)] = fake_pipeline
        assert manager.pipeline_get(pipeline) == fake_pipeline

    @pytest.mark.parametrize(
        'pipeline', ('Hello', FakePipeline('2')),
    )
    def test_pipeline_remove(self, pipeline):
        manager = Manager()
        fake_pipeline = FakePipeline(0)
        manager._pipelines[getattr(pipeline, 'id', pipeline)] = fake_pipeline
        manager.pipeline_remove(pipeline)
        assert pipeline not in manager._pipelines

    @freeze_time('2020-02-02 12:00:00')
    def test_transform_pipeline_process(self, app_with_db):
        manager = Manager(dbi=app_with_db.dbi)
        manager._pipelines['fake_pipeline'] = PipelineConfig(
            pipeline=FakePipeline(1234), sub_directory=None, force=False
        )
        manager.pipeline_process('fake_pipeline')
        actual_data_file_registry_list = DatafileRegistryModel.query.all()
        assert len(actual_data_file_registry_list) == 1
        actual_data_file_registry = actual_data_file_registry_list[0]
        assert actual_data_file_registry.state == DatafileState.PROCESSED.value
        assert actual_data_file_registry.error_message is None
        assert actual_data_file_registry.file_name is None
        assert actual_data_file_registry.created_timestamp == datetime.utcnow()
        assert actual_data_file_registry.updated_timestamp == datetime.utcnow()
        assert actual_data_file_registry.source == '1234'

    @pytest.mark.parametrize(
        'raise_exception,expected_state,expected_error_message',
        (
            (None, DatafileState.PROCESSED.value, None),
            ('Processing Error', DatafileState.FAILED.value, 'Processing Error'),
        ),
    )
    @freeze_time('2020-02-02 12:00:00')
    @mock.patch.object(DSSDatafileProvider, 'get_file_names')
    @mock.patch.object(DSSDatafileProvider, 'read_files')
    def test_pipeline_process(
        self,
        mock_read_files,
        mock_get_file_names,
        app_with_db,
        raise_exception,
        expected_state,
        expected_error_message,
    ):
        def read_files(*args, **kwargs):
            yield ['fake_file.txt']

        mock_get_file_names.return_value = ['fake_file.txt']
        mock_read_files.side_effect = read_files

        bucket = app_with_db.config['s3']['bucket_url']
        source_folder = os.path.join(bucket, app_with_db.config['s3']['datasets_folder'])
        manager = Manager(storage=source_folder, dbi=app_with_db.dbi)
        manager._pipelines['fake_pipeline'] = PipelineConfig(
            pipeline=FakePipeline(1234, raise_processing_exception=raise_exception),
            sub_directory='/tmp/fake_pipeline',
            force=False,
        )
        manager.pipeline_process('fake_pipeline')
        actual_data_file_registry_list = DatafileRegistryModel.query.all()
        assert len(actual_data_file_registry_list) == 1
        actual_data_file_registry = actual_data_file_registry_list[0]
        assert actual_data_file_registry.state == expected_state
        assert actual_data_file_registry.error_message == expected_error_message
        assert actual_data_file_registry.file_name == 'fake_file.txt'
        assert actual_data_file_registry.created_timestamp == datetime.utcnow()
        assert actual_data_file_registry.updated_timestamp == datetime.utcnow()
        assert actual_data_file_registry.source == '1234'

    @mock.patch.object(Manager, 'pipeline_process')
    def test_pipeline_process_all(self, mock_pipeline_process):
        mock_pipeline_process.return_value = None
        manager = Manager()
        manager._pipelines['fake_pipeline'] = None
        manager._pipelines['fake_pipeline_2'] = None
        manager.pipeline_process_all()
        assert mock_pipeline_process.call_args_list[0][0][0] == 'fake_pipeline'
        assert mock_pipeline_process.call_args_list[1][0][0] == 'fake_pipeline_2'

    def test_pipeline_register(self):
        manager = Manager()
        manager.pipeline_register(
            'test_pipeline', pipeline_id='fake_pipeline', custom_parameter=True, force=True
        )
        assert len(manager._pipelines) == 1
        self.assert_pipeline_config(manager, 'fake_pipeline', True, 'test_pipeline', None)

    def test_pipeline_register_when_already_registered(self):
        manager = Manager()
        manager.pipeline_register('test_pipeline', pipeline_id='fake_pipeline')
        with pytest.raises(ValueError):
            manager.pipeline_register('test_pipeline', pipeline_id='fake_pipeline')
        self.assert_pipeline_config(manager, 'fake_pipeline', False, 'test_pipeline', None)

    def assert_pipeline_config(
        self, manager, pipeline, expected_force, expected_pipeline, expected_sub_directory
    ):
        assert pipeline in manager._pipelines
        assert manager._pipelines[pipeline].force == expected_force
        assert manager._pipelines[pipeline].pipeline == expected_pipeline
        assert manager._pipelines[pipeline].sub_directory == expected_sub_directory
