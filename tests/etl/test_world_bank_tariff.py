from datatools.io.fileinfo import FileInfo

from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline
from app.etl.transforms.world_bank_tariff import CleanWorldBankTariff
from tests.utils import rows_equal_table

file_1 = 'tests/fixtures/world_bank/tariff.csv'


class TestWorldBankTariffTransform:

    def add_row(self, pipeline, app):
        sql = f"insert into {pipeline._l0_temp_table} (product, reporter, partner, tariff_year) " \
              f"values (1, 1, 2, 2016), (1, 1, 2, 2016), (1, 2, 2, 2014)"
        self.execute_sql(app, sql)

    def execute_sql(self, app, sql, return_results=False):
        connection = app.db.engine.connect()
        results = connection.execute(sql)
        if return_results:
            return [r for r in results]

    def test_get_partner_reporter_combinations_for_each_year(self, app_with_db):
        pipeline = WorldBankTariffPipeline(app_with_db.dbi)
        pipeline.create_tables()
        self.add_row(pipeline, app_with_db)

        clean_bank = CleanWorldBankTariff(
            pipeline._l0_temp_table,
            pipeline._l1_temp_table
        )

        sql = clean_bank.get_partner_reporter_combinations_for_each_year('table_1')
        wrapped_sql = f'with {sql} select * from table_1'
        results = self.execute_sql(app_with_db, wrapped_sql, return_results=True)

        assert [r for r in results] == [
            (1, 2, 1, 2016, 0),
            (1, 2, 1, 2017, 0),
            (1, 2, 1, 2018, 0),
        ]


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
