import io
from unittest import mock

import pytest
from tabulator import Stream

from app.uploader.csv_parser import CSVParser


@pytest.mark.parametrize(
    'headers,unique_headers',
    (
        (['hello', 'goodbye'], ['hello', 'goodbye']),
        (['hello', 'hello', 'hello', 'goodbye'], ['hello_1', 'hello_2', 'hello_3', 'goodbye']),
        (
            ['hello', 'hello', 'hello_1', 'goodbye'],
            ['hello_1_1', 'hello_2', 'hello_1_2', 'goodbye'],
        ),
    ),
)
def test_make_unique_headers(headers, unique_headers):
    assert CSVParser.make_unique_headers(headers) == unique_headers


@pytest.mark.parametrize(
    'csv_string,delimiter,quotechar',
    (
        ('hello,goodbye\n"1,1,1",2\n3,4\n5,6', ',', '"'),
        ('hello~goodbye\n1~2\n3~4\n5~6', '~', '"'),
        ('hello,goodbye\n$1,1,1$,2\n3,4\n5,6', ',', '$'),
    ),
)
@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample(mock_tabulator_stream, csv_string, delimiter, quotechar, app):
    _mock_stream_return_values(mock_tabulator_stream, [csv_string], delimiter, quotechar)
    result, err = CSVParser.get_csv_sample('', delimiter, number_of_lines_sample=2)
    assert not err
    assert len(result) == 2


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_when_extra_data_column(mock_tabulator_stream, app):
    csv_string = 'hello,goodbye\n1,2,3\n4,5,6\n7,8,9'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')

    assert err == (
        'Unable to process CSV file: row 2 has a different number of '
        'data points (3) than there are column headers (2)'
    )
    assert not result


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_when_empty_headers(mock_tabulator_stream, app):
    csv_string = 'hello,,goodbye,\n1,2,3,4\n5,6,7,8'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not err

    # column with empty header is ignored
    assert result == [('hello', 'integer', ['1', '5']), ('goodbye', 'integer', ['3', '7'])]


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_when_duplicate_header_names(mock_tabulator_stream, app):
    csv_string = 'hello,goodbye,goodbye\n1,2,3\n4,5,6\n7,8,9'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not err

    # duplicate headers are made unique
    assert result == [
        ('hello', 'integer', ['1', '4', '7']),
        ('goodbye_1', 'integer', ['2', '5', '8']),
        ('goodbye_2', 'integer', ['3', '6', '9']),
    ]


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_with_invalid_header_names(mock_tabulator_stream, app):
    csv_string = 'spaces in header,weird :@£$% characters,Uppercase\n1,2,3\n4,5,6\n7,8,9'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert err == (
        'Unable to process CSV file: column headers must start with a letter and may only '
        'contain lowercase letters, numbers, and underscores. Invalid headers: "spaces in '
        'header", "weird :@£$% characters", "Uppercase"'
    )
    assert not result


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_with_no_data(mock_tabulator_stream, app):
    csv_string = 'hello,goodbye'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert err == ('Unable to process CSV file: no data found')
    assert not result


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_empty_file(mock_tabulator_stream, app):
    csv_string = ' '
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not result
    assert err == (
        'Unable to process CSV file: no headers found. The first line of the csv should '
        'contain the column headers.'
    )


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_invalid_lines(mock_tabulator_stream, app):
    csv_string = 'hello,goodbye\nbad\n1,2'
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not result
    assert err == (
        'Unable to process CSV file: row 2 has a different number of '
        'data points (1) than there are column headers (2)'
    )


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_infer_data_types(mock_tabulator_stream, app):
    csv_string = (
        'int,bool,text,datetime,date,numeric,mix\n'
        '2000,true,test,2006-11-26T16:30:00Z,2004-01-01,3.1,test\n'
        '13,false,test,2018-12-18T12:10:00Z,1998-12-26,-1,-2'
    )
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not err
    assert result == [
        ('int', 'integer', ['2000', '13']),
        ('bool', 'boolean', ['true', 'false']),
        ('text', 'text', ['test', 'test']),
        ('datetime', 'timestamp', ['2006-11-26T16:30:00Z', '2018-12-18T12:10:00Z']),
        ('date', 'date', ['2004-01-01', '1998-12-26']),
        ('numeric', 'numeric', ['3.1', '-1']),
        ('mix', 'text', ['test', '-2']),
    ]


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_infer_data_types_null_values(mock_tabulator_stream, app):
    csv_string = (
        'int,bool,text,datetime,date,numeric,mix\n'
        ',NULL,test,2006-11-26T16:30:00Z,None,3.1,test\n'
        '13,false,test,2018-12-18T12:10:00Z,1998-12-26,-1,-2'
    )
    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not err
    assert result == [
        ('int', 'integer', ['', '13']),
        ('bool', 'boolean', ['', 'false']),
        ('text', 'text', ['test', 'test']),
        ('datetime', 'timestamp', ['2006-11-26T16:30:00Z', '2018-12-18T12:10:00Z']),
        ('date', 'date', ['', '1998-12-26']),
        ('numeric', 'numeric', ['3.1', '-1']),
        ('mix', 'text', ['test', '-2']),
    ]


@mock.patch('app.uploader.csv_parser.tabulator.Stream')
def test_get_s3_file_sample_infer_data_types_big_sample(mock_tabulator_stream, app):
    csv_string = 'int,bool,text,datetime,date,numeric,mix\n'
    for i in range(1000):
        if i == 900:
            csv_string += 'text,text,text,text,text,text,text\n'
            continue
        csv_string += '2000,true,test,2006-11-26T16:30:00Z,2004-01-01,3.1,test\n'

    _mock_stream_return_values(mock_tabulator_stream, [csv_string])
    result, err = CSVParser.get_csv_sample('', ',')
    assert not err
    for column, type, _ in result:
        assert type == 'text'


def _mock_stream_return_values(mock, csv_strings, delimiter=",", quotechar='"', sample_size=4):
    streams = []
    for s in csv_strings:
        bio = io.BytesIO(bytes(s, encoding='utf-8'))
        streams.append(
            Stream(
                bio,
                format='csv',
                headers=1,
                sample_size=sample_size,
                delimiter=delimiter,
                quotechar=quotechar,
                ignore_blank_headers=True,
                force_parse=True,
                encoding='utf-8',
                post_parse=[CSVParser.check_and_clean_up_row],
            )
        )
    mock.side_effect = streams
