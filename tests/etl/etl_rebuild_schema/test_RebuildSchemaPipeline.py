from unittest import mock

import pytest

from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


@pytest.fixture
def mock_BytesIO():
    path = 'app.etl.etl_rebuild_schema.BytesIO'
    with mock.patch(path) as mock_obj:
        yield mock_obj


@pytest.fixture
def mock_create_table():
    path = 'app.etl.etl_rebuild_schema._create_table'
    with mock.patch(path) as mock_create_table:
        yield mock_create_table


@pytest.fixture
def mock_RebuildSchemaPipeline_create_table():
    path = 'app.etl.etl_rebuild_schema.RebuildSchemaPipeline._create_table'
    with mock.patch(path) as mock_create_table:
        yield mock_create_table


@pytest.fixture
def mock_RebuildSchemaPipeline_datafile_to_table():
    path = 'app.etl.etl_rebuild_schema.RebuildSchemaPipeline._datafile_to_table'
    with mock.patch(path) as mock_datafile_to_table:
        yield mock_datafile_to_table


class TestCreateTable:
    def test(self, mock_create_table):
        dbi = mock.Mock()
        
        pipeline = RebuildSchemaPipeline(dbi)
        pipeline._create_table()
        
        assert mock_create_table.called_once_with(
            pipeline.data_column_types,
            dbi,
            pipeline.table_name,
            drop_existing=True,
            unique_column_names=pipeline.unique_column_names,
        )


class TestDatafileToTable:
    def test(self, mock_BytesIO):
        dbi = mock.Mock()
        file_info = mock.Mock()
        file_info.data.read.return_value = b'test'

        pipeline = RebuildSchemaPipeline(dbi)
        pipeline._datafile_to_table(file_info)

        dbi.dsv_buffer_to_table.assert_called_once_with(
            csv_buffer=mock_BytesIO.return_value,
            fq_table_name=pipeline.table_name,
            columns=None,
            has_header=True,
            sep=',',
            quote='"',
        )


class TestProcess:
    def test(
        self, mock_RebuildSchemaPipeline_create_table, mock_RebuildSchemaPipeline_datafile_to_table
    ):
        dbi = mock.Mock()
        file_info = mock.Mock()
        
        pipeline = RebuildSchemaPipeline(dbi)
        pipeline.process(file_info)
        
        mock_RebuildSchemaPipeline_create_table.assert_called_once_with()
        mock_RebuildSchemaPipeline_datafile_to_table.assert_called_once_with(file_info)


class TestRebuildSchemaPipeline:
    def test_l_tables_are_not_used(self):
        dbi = mock.Mock()
        
        pipeline = RebuildSchemaPipeline(dbi)
        
        assert pipeline.dataset is None
        assert pipeline.l1_helper_columns is None
        assert pipeline.L0_TABLE is None
        assert pipeline.L1_TABLE is None
        assert pipeline.L2_TABLE is None
        assert pipeline.organisation is None
        assert pipeline.schema is None
