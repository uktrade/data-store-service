import io
from unittest import mock

import pandas
import pytest

from app.constants import DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR
from app.uploader.utils import get_s3_file_sample, save_column_types, upload_file
from tests.fixtures.factories import PipelineFactory


@pytest.mark.parametrize(
    'data,expected_column_types',
    (({}, []), ({'hello': [1], 'goodbye': [2]}, [['hello', 'text'], ['goodbye', 'text']])),
)
def test_save_column_types(data, expected_column_types, app_with_db):
    pipeline = PipelineFactory()
    df = pandas.DataFrame(data=data)
    save_column_types(pipeline, df)
    assert pipeline.column_types == expected_column_types


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample(mock_smart_open, app_with_db):
    csv_string = 'hello,goodbye\n1,2\n3,4\n5,6'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR, number_of_lines=2)
    assert result.empty is False
    assert result.columns.to_list() == ['hello', 'goodbye']
    assert len(result.index) == 2


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample_when_invalid_csv(mock_smart_open, app_with_db):
    csv_string = 'hello,goodbye\n1,2,3\n4,5,6\n7,8,9'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR)
    assert result.empty is True
    assert result.columns.to_list() == []
    assert len(result.index) == 0


@mock.patch('app.uploader.utils._upload_file_stream')
def test_upload_file(mock_upload_file_obj, app_with_db):
    pipeline = PipelineFactory(organisation='org', dataset='set')
    mock_upload_file_obj.return_value = None
    file_name = 'test.csv'
    actual_file_name = upload_file('', file_name, pipeline)
    assert mock_upload_file_obj.called is True
    assert actual_file_name == 'upload/org/set/test.csv'
