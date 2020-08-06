import io
from unittest import mock

import pandas
import pytest
from datatools.io.storage import S3Storage

from app.constants import DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR
from app.uploader.utils import get_column_types, get_s3_file_sample, upload_file


@pytest.mark.parametrize(
    'data,expected_column_types',
    (({}, []), ({'hello': [1], 'goodbye': [2]}, [('hello', 'text'), ('goodbye', 'text')])),
)
def test_save_column_types(data, expected_column_types, app_with_db):
    df = pandas.DataFrame(data=data)
    assert get_column_types(df) == expected_column_types


@pytest.mark.parametrize(
    'csv_string,delimiter,quotechar',
    (
        ('hello,goodbye\n"1,1,1",2\n3,4\n5,6', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR),
        ('hello~goodbye\n1~2\n3~4\n5~6', '~', DEFAULT_CSV_QUOTECHAR),
        ('hello,goodbye\n$1,1,1$,2\n3,4\n5,6', DEFAULT_CSV_DELIMITER, '$'),
    ),
)
@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample(mock_smart_open, csv_string, delimiter, quotechar, app_with_db):
    mock_smart_open.return_value = io.StringIO(csv_string)
    result, err = get_s3_file_sample('', delimiter, quotechar, number_of_lines=2)
    assert result.empty is False
    assert result.columns.to_list() == ['hello', 'goodbye']
    assert len(result.index) == 2
    assert not err


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample_when_empty_column(mock_smart_open, app_with_db):
    csv_string = 'hello,goodbye\n1,2,3\n4,5,6\n7,8,9'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result, err = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR)
    assert result.empty is True
    assert result.columns.to_list() == []
    assert len(result.index) == 0
    assert err == 'Invalid CSV: content length not matching header length'


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample_when_invalid_header(mock_smart_open, app_with_db):
    csv_string = 'hello,goodbye,\n1,2,3\n4,5,6\n7,8,9'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result, err = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR)
    assert result.empty is True
    assert result.columns.to_list() == []
    assert len(result.index) == 0
    assert err == 'Invalid CSV: empty header names not allowed'


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample_when_duplicate_header_names(mock_smart_open, app_with_db):
    csv_string = 'hello,goodbye,goodbye\n1,2,3\n4,5,6\n7,8,9'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result, err = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR)
    assert result.empty is True
    assert result.columns.to_list() == []
    assert len(result.index) == 0
    assert err == 'Invalid CSV: duplicate header names not allowed'


@mock.patch('app.uploader.utils.open')
def test_get_s3_file_sample_with_invalid_header_names(mock_smart_open, app_with_db):
    csv_string = 'spaces in header,weird :@£$% characters,Uppercase\n1,2,3\n4,5,6\n7,8,9'
    mock_smart_open.return_value = io.StringIO(csv_string)
    result, err = get_s3_file_sample('', DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR)
    assert result.empty is True
    assert result.columns.to_list() == []
    assert len(result.index) == 0
    assert err == (
        'Invalid CSV: column headers must start with a letter and may only contain lowercase '
        'letters, numbers, and underscores. Invalid headers: "spaces in header", '
        '"weird :@£$% characters", "Uppercase"'
    )


@mock.patch('app.uploader.utils.StorageFactory.create')
def test_upload_file(mock_create_storage, app_with_db):
    with mock.patch.object(S3Storage, 'write_file', return_value=None) as mock_write_file:
        mock_create_storage.return_value = S3Storage('s3://bucket')
        upload_file('test.csv', 'bla')
    assert mock_create_storage.called is True
    assert mock_write_file.called is True
