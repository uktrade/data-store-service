from datatools.io.fileinfo import FileInfo

from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from tests.utils import rows_equal_table

snapshot1 = 'tests/fixtures/dit/reference_postcodes/snapshot1.csv'


class TestDITReferencePostcodesPipeline:
    def test_one_datafile(self, app_with_db):
        pipeline = DITReferencePostcodesPipeline(app_with_db.dbi, False)
        fi = FileInfo.from_path(snapshot1)
        pipeline.process(fi)
        expected_rows = [
            (
                'AB10 1AA',
                'S12000033',
                'Aberdeen City',
                'S99999999',
                None,
                'S99999999',
                None,
                'S99999999',
                'Scotland',
                '394251',
                '0806376',
                '2011-09-01',
                '2016-10-01',
            ),
            (
                'HU4 7SW',
                'E06000011',
                'East Riding of Yorkshire',
                'E37000018',
                'Humber',
                'E37000039',
                'York, North Yorkshire and East Riding',
                'E12000003',
                'Yorkshire and The Humber',
                '504860',
                '0429160',
                '1980-01-01',
                None,
            ),
        ]

        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
