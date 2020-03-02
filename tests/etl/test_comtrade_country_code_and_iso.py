from datatools.io.fileinfo import FileInfo

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from tests.utils import rows_equal_table


class TestComtradeCountryCodeAndISOPipeline:
    def test_pipeline(self, app_with_db):
        pipeline = ComtradeCountryCodeAndISOPipeline(app_with_db.dbi)
        fi = FileInfo.from_path('tests/fixtures/comtrade/country_code_and_iso/country_list.csv')
        pipeline.process(fi)

        # check L0
        expected_rows = [
            (0, 'World', 'World', 'World', 'World', 'WL', 'WLD', '1962', '2061'),
            (4, 'Afghanistan', 'Afghanistan', 'Afghanistan', None, 'AF', 'AFG', '1962', '2061'),
            (
                899,
                'Areas, nes',
                'Areas, not elsewhere specified',
                'Areas, nes',
                None,
                None,
                None,
                '1962',
                '2061',
            ),
            (
                918,
                'European Union',
                'European Union',
                'European Union',
                None,
                'EU',
                'EUR',
                None,
                None,
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
