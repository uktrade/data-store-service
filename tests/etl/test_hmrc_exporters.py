from datatools.io.fileinfo import FileInfo

from app.etl.organisation.hmrc import HMRCExportersPipeline
from tests.utils import rows_equal_table


def test_raw_to_events(app_with_db):
    fi = FileInfo.from_path('tests/fixtures/hmrc/exporters/exporters_2016_mock.zip')
    pipeline = HMRCExportersPipeline(app_with_db.dbi)
    pipeline.process(fi)

    expected_rows = [
        (
            '2016-01-01 00:00:00',
            'aaa limited',
            '01 street name street line london',
            'BB11BB',
            ['12345670'],
        ),
        (
            '2016-01-01 00:00:00',
            'abc company ltd',
            '1 - 3  street avenue house name city name',
            'AA11AA',
            ['12345678', '23456789'],
        ),
        (
            '2016-02-01 00:00:00',
            'bbb limited',
            'aaa house 2 street name city county',
            'AA111AA',
            ['98765432', '12345678'],
        ),
    ]
    assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)


def test_loading_new_data_into_existing_schema(app_with_db):
    fi = FileInfo.from_path('tests/fixtures/hmrc/exporters/exporters_2016_mock.zip')
    pipeline = HMRCExportersPipeline(app_with_db.dbi)
    pipeline.process(fi)

    fi.data.seek(0)
    fi2 = FileInfo(fi.name + '2', fi.data)
    pipeline.process(fi2)

    # check L1 (when loading new data, it should be appended (not re-creating all schema))
    expected_rows = [
        (
            '2016-01-01 00:00:00',
            'aaa limited',
            '01 street name street line london',
            'BB11BB',
            ['12345670'],
        ),
        (
            '2016-01-01 00:00:00',
            'aaa limited',
            '01 street name street line london',
            'BB11BB',
            ['12345670'],
        ),
        (
            '2016-01-01 00:00:00',
            'abc company ltd',
            '1 - 3  street avenue house name city name',
            'AA11AA',
            ['12345678', '23456789'],
        ),
        (
            '2016-01-01 00:00:00',
            'abc company ltd',
            '1 - 3  street avenue house name city name',
            'AA11AA',
            ['12345678', '23456789'],
        ),
        (
            '2016-02-01 00:00:00',
            'bbb limited',
            'aaa house 2 street name city county',
            'AA111AA',
            ['98765432', '12345678'],
        ),
        (
            '2016-02-01 00:00:00',
            'bbb limited',
            'aaa house 2 street name city county',
            'AA111AA',
            ['98765432', '12345678'],
        ),
    ]
    assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
