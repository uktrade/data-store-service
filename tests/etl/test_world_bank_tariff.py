import pytest
from datatools.io.fileinfo import FileInfo

from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline
from tests.utils import rows_equal_table

fixture_path = 'tests/fixtures/world_bank'

file_1 = f'{fixture_path}/tariff.csv'
eu_country_to_eu_country_file = f'{fixture_path}/eu_country_to_eu_country.csv'
eu_to_country_file = f'{fixture_path}/eu_to_country.csv'

comtrade_countries = [
    {'id': 1, 'cty_code': 0, 'cty_name_english': 'World', 'iso3_digit_alp': 'WLD'},
    {'id': 2, 'cty_code': 48, 'cty_name_english': 'Bahrain', 'iso3_digit_alp': 'BHR'},
    {'id': 3, 'cty_code': 262, 'cty_name_english': 'Djibouti', 'iso3_digit_alp': 'DJI'},
    {'id': 4, 'cty_code': 266, 'cty_name_english': 'Gabon', 'iso3_digit_alp': 'GAB'},
    {'id': 5, 'cty_code': 381, 'cty_name_english': 'Italy', 'iso3_digit_alp': 'ITA'},
    {'id': 6, 'cty_code': 918, 'cty_name_english': 'European Union', 'iso3_digit_alp': 'EU'},
    {'id': 7, 'cty_code': 36, 'cty_name_english': 'Australia', 'iso3_digit_alp': 'AUS'},
    {'id': 8, 'cty_code': 705, 'cty_name_english': 'Slovenia', 'iso3_digit_alp': 'SVN'},
]
eu_country_memberships = [
    {'id': i, 'country': 'Italy', 'iso3': 'ITA', 'year': year, 'tariff_code': 'EUN'}
    for i, year in enumerate(range(1958, 2018), 1)
] + [
    {
        'id': i,
        'country': 'Slovenia',
        'iso3': 'ITA',
        'year': year,
        'tariff_code': 'EUN' if year >= 2004 else None,
    }
    for i, year in enumerate(range(1958, 2018), 61)
]


class TestWorldBankTariffPipeline:
    @pytest.fixture(autouse=True, scope='function')
    def setup(self, app_with_db, add_comtrade_country_code_and_iso, add_dit_eu_country_membership):
        self.dbi = app_with_db.dbi
        add_comtrade_country_code_and_iso(comtrade_countries)
        add_dit_eu_country_membership(eu_country_memberships)

    def test_pipeline(self):
        pipeline = WorldBankTariffPipeline(self.dbi, True)
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

        assert rows_equal_table(self.dbi, expected_rows, pipeline._l0_table, pipeline, top_rows=1)

        # check L1
        assert rows_equal_table(self.dbi, [], pipeline._l1_table, pipeline, top_rows=1)

    @pytest.mark.parametrize(
        'file_name,years,expected_rows',
        (
            (
                eu_country_to_eu_country_file,
                [2014],
                [(201, 705, 381, 2014, 76.51, 76.51, 76.53, 76.54, None, 76.51, 76.51)],
                # Italy has incorrect id 380 in product file and has to be fixed by the
                # cleaning process and updated to 381
            ),
            (
                eu_to_country_file,
                [2014],
                [(201, 918, 36, 2014, 81.96, 81.96, 81.96, 81.96, 83.03, 81.96, 81.96)],
                # When the reporter is EU it remains EU
            ),
        ),
    )
    def test_transform_of_datafile(self, file_name, years, expected_rows):
        pipeline = WorldBankTariffPipeline(self.dbi, True)
        fi = FileInfo.from_path(file_name)
        pipeline.process(fi)
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline)
