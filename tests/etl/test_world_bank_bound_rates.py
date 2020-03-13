from datatools.io.fileinfo import FileInfo

from app.etl.etl_world_bank_bound_rates import WorldBankBoundRatesPipeline
from tests.utils import rows_equal_table


class TestWorldBankBoundRatesPipeline:
    def test_pipeline(self, app_with_db):
        pipeline = WorldBankBoundRatesPipeline(app_with_db.dbi)
        fi = FileInfo.from_path('tests/fixtures/world_bank/bound_rates.csv')
        pipeline.process(fi)

        # check L0
        expected_rows = [
            ('H0', 4, 10111, 10, 2),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        expected_rows = [
            (4, 10111, 10),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)