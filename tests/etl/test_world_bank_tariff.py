import pytest

from unittest import mock

from datatools.io.fileinfo import FileInfo

from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline
from app.etl.transforms.world_bank_tariff import CleanWorldBankTariff
from tests.utils import rows_equal_table

file_1 = 'tests/fixtures/world_bank/tariff.csv'
eu_country_to_eu_country_file = 'tests/fixtures/world_bank/eu_country_to_eu_country.csv'


class TestWorldBankTariffTransform:
    def add_basic_rows(self, _app, pipeline):
        sql = (
            f"insert into {pipeline._l0_temp_table} (product, reporter, partner, tariff_year) "
            f"values (1, 1, 2, 2016), (1, 1, 2, 2016), (1, 2, 2, 2014), (1, 3, 2, 2014)"
        )
        self.execute_sql(_app, sql)

    def add_rows(self, _app, pipeline):
        sql = (
            f"insert into {pipeline._l0_temp_table} (product, reporter, partner, tariff_year, "
            f"duty_type, simple_average"
            f") "
            f"values (1, 1, 2, 2016, 'BND', 1.5)"
        )
        self.execute_sql(_app, sql)

    def execute_sql(self, _app, sql, return_results=False):
        connection = _app.db.engine.raw_connection()
        cur = connection.cursor()
        cur.execute(sql)
        results = []
        if return_results:
            results = cur.fetchall()
        connection.commit()
        cur.close()
        connection.close()
        return [r for r in results]

    def test_get_partner_reporter_combinations_for_each_year(self, app_with_db):
        pipeline = WorldBankTariffPipeline(app_with_db.dbi, False)
        pipeline.dbi.append_table(pipeline._l0_temp_table, pipeline._l0_table, drop_source=True)
        pipeline.create_tables()
        self.add_basic_rows(app_with_db, pipeline)

        clean_bank = CleanWorldBankTariff(pipeline._l0_temp_table, pipeline._l1_temp_table)

        sql = clean_bank.get_partner_reporter_combinations_for_each_year('table_1')
        wrapped_sql = (
            f'with {sql} select reporter,partner,year from table_1 order by reporter, ' f'year'
        )
        results = self.execute_sql(app_with_db, wrapped_sql, return_results=True)
        assert results == [
            (1, 2, 2016),
            (1, 2, 2017),
            (1, 2, 2018),
            (3, 2, 2014),
            (3, 2, 2015),
            (3, 2, 2016),
            (3, 2, 2017),
            (3, 2, 2018),
        ]

    @mock.patch("app.etl.transforms.world_bank_tariff.CleanWorldBankTariff.get_years")
    def test_get_rate_averages(self, mock_get_years, app_with_db):
        mock_get_years.return_value = [2016]
        pipeline = WorldBankTariffPipeline(app_with_db.dbi, False)
        pipeline.dbi.append_table(pipeline._l0_temp_table, pipeline._l0_table, drop_source=True)
        pipeline.create_tables()
        self.add_rows(app_with_db, pipeline)

        clean_bank = CleanWorldBankTariff(pipeline._l0_temp_table, pipeline._l1_temp_table)

        year_sql = clean_bank.get_partner_reporter_combinations_for_each_year('table_1')
        sql = clean_bank.get_rate_averages('table_2', 'table_1')
        wrapped_sql = (
            f'with {year_sql}, {sql} select reporter, partner, year, bnd_rate from ' f'table_2'
        )

        results = self.execute_sql(app_with_db, wrapped_sql, return_results=True)
        assert results == [(1, 2, 2016, '1.5')]


class TestWorldBankTariffPipeline:
    def test_one_datafile(self, app_with_db):
        pipeline = WorldBankTariffPipeline(app_with_db.dbi, True)
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

    @pytest.mark.parametrize(
        'file_name,years,expected_rows',
        (
            (
                eu_country_to_eu_country_file,
                [2014],
                [(201, 705, 381, 2014, '76.51', '76.51', '76.53', '76.54', 'NA', 76.51, 76.51)],
                # Italy has incorrect id 380 in product file and has to be fixed by the
                # cleaning process and updated to 381
            ),
        ),
    )
    @mock.patch("app.etl.transforms.world_bank_tariff.CleanWorldBankTariff.get_years")
    def test_transform_of_datafile(
        self, mock_get_years, app_with_db, file_name, years, expected_rows
    ):
        mock_get_years.return_value = years
        pipeline = WorldBankTariffPipeline(app_with_db.dbi, True)
        fi = FileInfo.from_path(eu_country_to_eu_country_file)
        pipeline.process(fi)
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
