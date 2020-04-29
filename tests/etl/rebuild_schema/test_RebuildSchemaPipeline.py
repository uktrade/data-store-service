import csv
import io
from unittest import mock

import pytest

from app.etl.pipeline_type.rebuild_schema import RebuildSchemaPipeline


def convert_to_csv_bytes(fieldnames, rows):
    data = [{field: value for field, value in zip(fieldnames, row)} for row in rows]
    f = io.StringIO()
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
    f.seek(0)
    data_text = f.read()
    data_bytes = data_text.encode()
    return data_bytes


@pytest.fixture
def mock_sql_alchemy_model():
    path = 'app.etl.pipeline_type.rebuild_schema.RebuildSchemaPipeline.sql_alchemy_model'
    with mock.patch(path) as mock_obj:
        yield mock_obj


class TestProcess:
    def test_creation_of_model_objects(self, app_with_db, mock_sql_alchemy_model):

        dbi = mock.Mock()
        fieldnames = ['a', 'b']
        rows = [[0, 1], [2, 3]]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = RebuildSchemaPipeline(dbi)
        pipeline.process(file_info)

        calls = mock_sql_alchemy_model.call_args_list
        expected = [mock.call(a='0', b='1'), mock.call(a='2', b='3')]

        assert calls == expected

    def test_mapping(self, app_with_db, mock_sql_alchemy_model):

        dbi = mock.Mock()
        fieldnames = ['a', 'b']
        rows = [[0, 1], [2, 3]]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = RebuildSchemaPipeline(dbi)
        pipeline.csv_to_model_mapping = {'a': 'c', 'b': 'd'}
        pipeline.process(file_info)

        calls = mock_sql_alchemy_model.call_args_list
        expected = [mock.call(c='0', d='1'), mock.call(c='2', d='3')]

        assert calls == expected

    def test_null_values_converted_to_none(self, app_with_db, mock_sql_alchemy_model):

        dbi = mock.Mock()
        fieldnames = ['a', 'b']
        rows = [[0, 'null'], [1, 'apple'], [2, 'pineapple'], ]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = RebuildSchemaPipeline(dbi)
        pipeline.null_values = ['null', 'pineapple']
        pipeline.process(file_info)

        calls = mock_sql_alchemy_model.call_args_list
        expected = [
            mock.call(a='0', b=None),
            mock.call(a='1', b='apple'),
            mock.call(a='2', b=None)
        ]

        assert calls == expected
        
