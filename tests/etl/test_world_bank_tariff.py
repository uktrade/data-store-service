from datatools.io.fileinfo import FileInfo

from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline
from tests.utils import rows_equal_table

file_1 = 'tests/fixtures/world_bank/tariff.csv'


class TestWorldBankTariffPipeline:
    def test_one_datafile(self, app_with_db):
        pipeline = WorldBankTariffPipeline(app_with_db.dbi, False)
        fi = FileInfo.from_path(file_1)
        pipeline.process(fi)
        # check L0
        expected_rows = [
            (
                'HS',
                'H0',
                48,
                'Bahrain',
                201,
                'Meat of bovine animals, fresh or chilled.',
                0,
                ' World',
                1999,
                1999,
                'WTO',
                'AHS',
                5.0,
                5.0,
                0.0,
                5.0,
                5.0,
                20,
                0,
                0,
                1649.213,
                None,
                5.0,
                0.0,
                100.0,
                100.0,
                20,
                500.0,
                0,
                0,
                0,
                20,
                20,
                0,
                0,
                0,
                0,
                0,
                8246.063,
                1649.213,
                0.0,
                1649.213,
                0.0,
            )
        ]

        assert rows_equal_table(
            app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline, top_rows=1
        )
