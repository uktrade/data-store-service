import datetime
from decimal import Decimal
from unittest import mock

from datatools.io.fileinfo import FileInfo

from app.etl.organisation.companies_house import CompaniesHouseAccountsPipeline


def test_process_ch_accounts_datafile(app_with_db):
    # run tests
    fi = FileInfo.from_path('tests/fixtures/companies_house/accounts/test_datafile_1.zip')
    pipeline = CompaniesHouseAccountsPipeline(app_with_db.dbi, trigger_dataflow_dag=True)
    pipeline.process(fi)

    # check output of l0
    expected_rows_l0 = [
        (
            1,
            'test_datafile_1.zip',
            'Prod223_1859',
            '1234',
            datetime.date(2017, 2, 27),
            'xml',
            'http://www.xbrl.org/uk/fr/gaap/pt/2004-12-01',
            datetime.date(2016, 7, 31),
            datetime.date(2016, 7, 31),
            datetime.date(2016, 7, 31),
            '1234',
            'BadCorp Limited',
            False,
            None,
            Decimal('251044.0'),
            Decimal('1840.0'),
            Decimal('29277.0'),
            Decimal('31117.0'),
            None,
            None,
            Decimal('16747.0'),
            Decimal('267791.0'),
            Decimal('267734.0'),
            Decimal('5700.0'),
            Decimal('18511.0'),
            Decimal('267734.0'),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            2,
            'test_datafile_1.zip',
            'Prod223_1859',
            '1234',
            datetime.date(2017, 2, 27),
            'xml',
            'http://www.xbrl.org/uk/fr/gaap/pt/2004-12-01',
            datetime.date(2015, 7, 31),
            datetime.date(2015, 7, 31),
            datetime.date(2016, 7, 31),
            '1234',
            'BadCorp Limited',
            False,
            None,
            Decimal('251160.0'),
            Decimal('2162.0'),
            Decimal('32909.0'),
            Decimal('35071.0'),
            None,
            None,
            Decimal('18604.0'),
            Decimal('269764.0'),
            Decimal('269707.0'),
            Decimal('5700.0'),
            Decimal('20484.0'),
            Decimal('269707.0'),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]
    rows_l0 = app_with_db.dbi.execute_query(f'SELECT * FROM {pipeline._l0_table} order by id')
    assert rows_l0 == expected_rows_l0


@mock.patch('app.utils.hawk_api_request')
def test_trigger_dataflow_dag(mock_api_request, mocker, app):
    mocker.patch.dict(
        app.config,
        {
            'dataflow': {
                'hawk_id': 'test_id',
                'hawk_key': 'test_key',
                'data_uploader_dag_id': 'DSSGenericPipeline',
                'base_url': 'http://localhost:8080',
            }
        },
    )

    pipeline = CompaniesHouseAccountsPipeline(None, trigger_dataflow_dag=True)
    pipeline.trigger_dataflow_dag()

    mock_api_request.assert_called_once_with(
        'http://localhost:8080/api/experimental/dags/DSSGenericPipeline/dag_runs',
        'POST',
        {'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
        {
            'conf': {
                'data_uploader_schema_name': 'companies_house.accounts',
                'data_uploader_table_name': 'L0',
            }
        },
    )
