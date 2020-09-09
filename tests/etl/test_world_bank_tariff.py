from unittest import mock

import pytest
from data_engineering.common.tests.conftest import create_tables
from datatools.io.fileinfo import FileInfo

from app.etl.organisation.world_bank import (
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline,
)
from tests.utils import rows_equal_table

fixture_path = 'tests/fixtures/world_bank'

file_1 = f'{fixture_path}/tariff.csv'
country_to_country_three_products = f'{fixture_path}/country_to_country_three_products.csv'


PRODUCT_201_ROWS = [
    (201, 12, 24, 2018, None, None, None, None, None, None, None, None),
    (201, 12, 36, 2018, None, None, None, None, None, None, None, None),
    (201, 12, 76, 2018, 21, 21, None, None, None, None, None, None),
    (201, 12, 710, 2018, None, None, None, None, None, None, None, None),
    (201, 24, 12, 2018, None, None, None, None, None, None, None, None),
    (201, 24, 36, 2018, None, None, None, None, None, None, None, None),
    (201, 24, 76, 2018, None, None, None, None, None, None, None, None),
    (201, 24, 710, 2018, None, None, None, None, None, None, None, None),
    (201, 36, 12, 2018, None, None, None, None, None, None, None, None),
    (201, 36, 24, 2018, None, None, None, None, None, None, None, None),
    (201, 36, 76, 2018, None, None, None, None, None, None, None, None),
    (201, 36, 710, 2018, None, None, None, None, None, None, None, None),
    (201, 76, 12, 2018, None, None, None, None, None, None, None, None),
    (201, 76, 24, 2018, None, None, None, None, None, None, None, None),
    (201, 76, 36, 2018, None, None, None, None, None, None, None, None),
    (201, 76, 710, 2018, None, None, None, None, None, None, None, None),
    (201, 710, 12, 2018, None, None, None, None, None, None, None, None),
    (201, 710, 24, 2018, None, None, None, None, None, None, None, None),
    (201, 710, 36, 2018, None, None, None, None, None, None, None, None),
    (201, 710, 76, 2018, None, None, None, None, None, None, None, None),
]

PRODUCT_301_ROWS = [
    (301, 12, 24, 2018, None, None, None, None, None, None, None, None),
    (301, 12, 36, 2018, None, None, None, None, None, None, None, None),
    (301, 12, 76, 2018, None, None, None, None, None, None, None, None),
    (301, 12, 710, 2018, None, None, None, None, None, None, None, None),
    (301, 24, 12, 2018, None, None, None, None, None, None, None, None),
    (301, 24, 36, 2018, None, None, None, None, None, None, None, None),
    (301, 24, 76, 2018, None, None, None, None, None, None, None, None),
    (301, 24, 710, 2018, 10, 10, None, None, None, None, None, None),
    (301, 36, 12, 2018, None, None, None, None, None, None, None, None),
    (301, 36, 24, 2018, None, None, None, None, None, None, None, None),
    (301, 36, 76, 2018, None, None, None, None, None, None, None, None),
    (301, 36, 710, 2018, None, None, None, None, None, None, None, None),
    (301, 76, 12, 2018, None, None, None, None, None, None, None, None),
    (301, 76, 24, 2018, None, None, None, None, None, None, None, None),
    (301, 76, 36, 2018, None, None, None, None, None, None, None, None),
    (301, 76, 710, 2018, None, None, None, None, None, None, None, None),
    (301, 710, 12, 2018, None, None, None, None, None, None, None, None),
    (301, 710, 24, 2018, None, None, None, None, None, None, None, None),
    (301, 710, 36, 2018, None, None, None, None, None, None, None, None),
    (301, 710, 76, 2018, None, None, None, None, None, None, None, None),
]

PRODUCT_401_ROWS = [
    (401, 12, 24, 2018, None, None, None, None, None, None, None, None),
    (401, 12, 36, 2018, None, None, None, None, None, None, None, None),
    (401, 12, 76, 2018, None, None, None, None, None, None, None, None),
    (401, 12, 710, 2018, None, None, None, None, None, None, None, None),
    (401, 24, 12, 2018, None, None, None, None, None, None, None, None),
    (401, 24, 36, 2018, None, None, None, None, None, None, None, None),
    (401, 24, 76, 2018, None, None, None, None, None, None, None, None),
    (401, 24, 710, 2018, None, None, None, None, None, None, None, None),
    (401, 36, 12, 2018, None, None, None, None, None, None, None, None),
    (401, 36, 24, 2018, None, None, None, None, None, None, None, None),
    (401, 36, 76, 2018, None, None, None, None, None, None, None, None),
    (401, 36, 710, 2018, 10, 10, None, None, None, None, None, None),
    (401, 76, 12, 2018, None, None, None, None, None, None, None, None),
    (401, 76, 24, 2018, None, None, None, None, None, None, None, None),
    (401, 76, 36, 2018, None, None, None, None, None, None, None, None),
    (401, 76, 710, 2018, None, None, None, None, None, None, None, None),
    (401, 710, 12, 2018, None, None, None, None, None, None, None, None),
    (401, 710, 24, 2018, None, None, None, None, None, None, None, None),
    (401, 710, 36, 2018, None, None, None, None, None, None, None, None),
    (401, 710, 76, 2018, None, None, None, None, None, None, None, None),
]

comtrade_countries = [
    {'id': 1, 'cty_code': 0, 'cty_name_english': 'World', 'iso3_digit_alpha': 'WLD'},
    {'id': 2, 'cty_code': 48, 'cty_name_english': 'Bahrain', 'iso3_digit_alpha': 'BHR'},
    {'id': 3, 'cty_code': 262, 'cty_name_english': 'Djibouti', 'iso3_digit_alpha': 'DJI'},
    {'id': 4, 'cty_code': 266, 'cty_name_english': 'Gabon', 'iso3_digit_alpha': 'GAB'},
    {'id': 5, 'cty_code': 380, 'cty_name_english': 'Italy', 'iso3_digit_alpha': 'ITAF'},
    {'id': 6, 'cty_code': 918, 'cty_name_english': 'European Union', 'iso3_digit_alpha': 'EU'},
    {'id': 7, 'cty_code': 36, 'cty_name_english': 'Australia', 'iso3_digit_alpha': 'AUS'},
    {'id': 8, 'cty_code': 705, 'cty_name_english': 'Slovenia', 'iso3_digit_alpha': 'SVN'},
    {'id': 9, 'cty_code': 724, 'cty_name_english': 'Spain', 'iso3_digit_alpha': 'ESP'},
    {'id': 10, 'cty_code': 24, 'cty_name_english': 'Angola', 'iso3_digit_alpha': 'AGO'},
    {'id': 11, 'cty_code': 76, 'cty_name_english': 'Brazil', 'iso3_digit_alpha': 'BRA'},
    {'id': 12, 'cty_code': 710, 'cty_name_english': 'South Africa', 'iso3_digit_alpha': 'ZAF'},
    {'id': 13, 'cty_code': 12, 'cty_name_english': 'Algeria', 'iso3_digit_alpha': 'DZA'},
]

eu_country_memberships = [
    {'id': i, 'country': 'Italy', 'iso3': 'ITA', 'year': year, 'tariff_code': 'EUN'}
    for i, year in enumerate(range(1958, 2021), 1)
]
eu_country_memberships += [
    {
        'id': i,
        'country': 'Slovenia',
        'iso3': 'SVN',
        'year': year,
        'tariff_code': 'EUN' if year >= 2004 else None,
    }
    for i, year in enumerate(range(1958, 2021), len(eu_country_memberships) + 1)
]
eu_country_memberships += [
    {
        'id': i,
        'country': 'Spain',
        'iso3': 'ESP',
        'year': year,
        'tariff_code': 'EUN' if year >= 1958 else None,
    }
    for i, year in enumerate(range(1958, 2021), len(eu_country_memberships) + 1)
]


class TestWorldBankTariffPipeline:
    @pytest.fixture(autouse=True, scope='function')
    def setup(
        self, app_with_db, add_comtrade_country_code_and_iso, add_dit_eu_country_membership,
    ):
        self.dbi = app_with_db.dbi
        self.dbi.execute_statement('drop table if exists "dit.baci"."L1" cascade;')
        create_tables(app_with_db)
        add_comtrade_country_code_and_iso(comtrade_countries)
        add_dit_eu_country_membership(eu_country_memberships)

    def test_pipeline(self, add_world_bank_raw_tariff):
        pipeline = WorldBankTariffPipeline(self.dbi, force=True)
        fi = FileInfo.from_path(file_1)
        pipeline.process(fi)

        # check L0
        expected_rows = [
            (48, 1999, 201, 0, 'AHS', 5, 20),
            (262, 2005, 201, 380, 'BND', 40, 40),
            (266, 1998, 201, 0, 'AHS', 20, 20),
        ]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        pipeline = WorldBankTariffTransformPipeline(self.dbi, force=True)
        pipeline.process()
        expected_rows = [
            (201, 12, 24, 2000, None, None, None, None, None, None, None, None),
            (201, 12, 24, 2001, None, None, None, None, None, None, None, None),
        ]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline, top_rows=2)

        # check second run with different raw tariff updates L1
        add_world_bank_raw_tariff(
            [
                {
                    'reporter': 12,
                    'year': 2000,
                    'product': 201,
                    'partner': 24,
                    'duty_type': 'AHS',
                    'simple_average': 10,
                    'number_of_total_lines': 8,
                },
            ]
        )
        pipeline.process()
        expected_rows = [
            (201, 12, 24, 2000, 10, 10, None, None, None, None, None, None),
            (201, 12, 24, 2001, 10, None, None, None, None, None, None, None),
        ]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline, top_rows=2)

    @pytest.mark.parametrize(
        'raw_tariffs,bound_tariffs,year_range,required_countries,only_products,expected_rows',
        (
            # country_to_country test
            (
                # raw tariffs
                [
                    {
                        'reporter': 705,
                        'year': 2018,
                        'product': 201,
                        'partner': 380,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 8,
                    },
                ],
                # bound tariffs
                [{'reporter': 705, 'product': 201, 'bound_rate': 80}],
                # year range
                ('2018', '2018'),
                # required countries
                [('SVN', 705, True), ('ITA', 380, True)],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                [
                    (201, 380, 705, 2018, 0, None, None, None, None, None, 0, None),
                    # EU - EU has zero rate
                    (201, 705, 380, 2018, 0, 10, None, 80, None, 10, 0, None),
                    # EU - EU has zero rate with priority on app rate
                    # Italy has different id 381 in comtrade list and should updated to 380
                ],
            ),
            # eu_to_country test
            (
                # raw tariffs
                [
                    {
                        'reporter': 918,
                        'year': 2017,
                        'product': 201,
                        'partner': 36,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 1,
                    },
                    {
                        'reporter': 705,
                        'year': 2017,
                        'product': 201,
                        'partner': 36,
                        'duty_type': 'AHS',
                        'simple_average': 20,
                        'number_of_total_lines': 1,
                    },
                    {
                        'reporter': 36,
                        'year': 2017,
                        'product': 201,
                        'partner': 705,
                        'duty_type': 'AHS',
                        'simple_average': 30,
                        'number_of_total_lines': 1,
                    },
                ],
                # bound tariffs
                [{'reporter': 705, 'product': 201, 'bound_rate': 80}],
                # year range
                ('2017', '2017'),
                # required countries
                [('SVN', 705, True), ('AUS', 36, True), ('ESP', 724, True)],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                #
                #  When the reporter is EU (918) it's reporter_rate needs to be expanded
                #  to all required eu countries (SVN, ESP)
                [
                    (201, 36, 705, 2017, 30, 30, None, None, None, 30, None, None),
                    # use app rate
                    (201, 36, 724, 2017, 30, None, None, None, None, 30, None, None),
                    # use average eu partner rate
                    (201, 705, 36, 2017, 20, 20, None, 80, 10, None, None, None),
                    # app rate has priority on eu_rep_rate (expansion 918-36 to 705-36)
                    (201, 705, 724, 2017, 0, None, None, 80, None, None, 0, None),
                    # EU - EU has zero rate
                    (201, 724, 36, 2017, 10, None, None, None, 10, None, None, None),
                    # eu_rep_rate expanded from 918-36 (AUS)
                    (201, 724, 705, 2017, 0, None, None, None, None, None, 0, None)
                    # EU - EU has zero rate and trumps app rate
                ],
            ),
            # country_to_world test
            (
                # raw tariffs
                [
                    {
                        'reporter': 12,
                        'year': 2017,
                        'product': 201,
                        'partner': 0,
                        'duty_type': 'MFN',
                        'simple_average': 30,
                        'number_of_total_lines': 17,
                    },
                    {
                        'reporter': 12,
                        'year': 2017,
                        'product': 201,
                        'partner': 76,
                        'duty_type': 'AHS',
                        'simple_average': 21,
                        'number_of_total_lines': 13,
                    },
                    {
                        'reporter': 24,
                        'year': 2017,
                        'product': 201,
                        'partner': 0,
                        'duty_type': 'MFN',
                        'simple_average': 5,
                        'number_of_total_lines': 8,
                    },
                    {
                        'reporter': 24,
                        'year': 2017,
                        'product': 201,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                ],
                # bound tariffs
                [],
                # year range
                ('2017', '2017'),
                # required countries
                [('BRA', 76, True), ('ZAF', 710, True), ('DZA', 12, True), ('AGO', 24, True)],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                #
                # Angola (24 - AGO) has a tariff specified with World & South Africa (710 - ZAF)
                # Algeria (12 - DZA) has a tariff specified with World & Brazil (76 - BRA)
                [
                    (201, 12, 24, 2017, 30, None, 30, None, None, None, None, 17.5),
                    # mfn_rate used from 12 (DZA) - 0 (WLD)
                    (201, 12, 76, 2017, 21, 21, 30, None, None, None, None, 17.5),
                    # ahs_rate from 12 (DZA) - 76 (BRA) priority on MFN from 12 (DZA) - 0 (WLD)
                    (201, 12, 710, 2017, 30, None, 30, None, None, None, None, 17.5),
                    # mfn_rate used from 12 (DZA) - 0 (WLD)
                    (201, 24, 12, 2017, 5, None, 5, None, None, None, None, 17.5),
                    # mfn_rate used from 24 (AGO) - 0 (WLD)
                    (201, 24, 76, 2017, 5, None, 5, None, None, None, None, 17.5),
                    # mfn_rate used from 24 (AGO) - 0 (WLD)
                    (201, 24, 710, 2017, 10, 10, 5, None, None, None, None, 17.5),
                    # ahs_rate from 24 (AGO) - 710 (ZAF) priority on MFN from 24 (AGO) - 0 (WLD)
                    (201, 76, 12, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                    (201, 76, 24, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                    (201, 76, 710, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                    (201, 710, 12, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                    (201, 710, 24, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                    (201, 710, 76, 2017, 17.5, None, None, None, None, None, None, 17.5),
                    # only world average available
                ],
            ),
            # country_to_country_two_years test
            (
                # raw tariffs
                [
                    {
                        'reporter': 12,
                        'year': 2017,
                        'product': 201,
                        'partner': 76,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 1,
                    },
                    {
                        'reporter': 12,
                        'year': 2018,
                        'product': 201,
                        'partner': 76,
                        'duty_type': 'AHS',
                        'simple_average': 20,
                        'number_of_total_lines': 1,
                    },
                ],
                # bound tariffs
                [],
                # year range
                ('2017', '2018'),
                # required countries
                [('BRA', 76, True), ('DZA', 12, True)],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                [
                    (201, 12, 76, 2017, 10, 10, None, None, None, None, None, None),
                    (201, 12, 76, 2018, 20, 20, None, None, None, None, None, None),
                    (201, 76, 12, 2017, None, None, None, None, None, None, None, None),
                    (201, 76, 12, 2018, None, None, None, None, None, None, None, None),
                ],
            ),
            # country_to_country_three_products test
            (
                # raw tariffs
                [
                    {
                        'reporter': 12,
                        'year': 2018,
                        'product': 201,
                        'partner': 76,
                        'duty_type': 'AHS',
                        'simple_average': 21,
                        'number_of_total_lines': 13,
                    },
                    {
                        'reporter': 24,
                        'year': 2018,
                        'product': 301,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 36,
                        'year': 2018,
                        'product': 401,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                ],
                # bound tariffs
                [],
                # year range
                ('2018', '2018'),
                # required countries
                [
                    ('BRA', 76, True),
                    ('ZAF', 710, True),
                    ('DZA', 12, True),
                    ('AGO', 24, True),
                    ('AUS', 36, True),
                ],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                PRODUCT_201_ROWS + PRODUCT_301_ROWS + PRODUCT_401_ROWS,
            ),
            # country_to_country_three_products
            (
                # raw tariffs
                [
                    {
                        'reporter': 12,
                        'year': 2018,
                        'product': 201,
                        'partner': 76,
                        'duty_type': 'AHS',
                        'simple_average': 21,
                        'number_of_total_lines': 13,
                    },
                    {
                        'reporter': 24,
                        'year': 2018,
                        'product': 301,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 36,
                        'year': 2018,
                        'product': 401,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                ],
                # bound tariffs
                [],
                # year range
                ('2018', '2018'),
                # required countries
                [
                    ('BRA', 76, True),
                    ('ZAF', 710, True),
                    ('DZA', 12, True),
                    ('AGO', 24, True),
                    ('AUS', 36, True),
                ],
                # products
                '201,401',
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                PRODUCT_201_ROWS + PRODUCT_401_ROWS,
            ),
            # forward_fill test
            (
                # raw tariffs
                [
                    {
                        'reporter': 918,
                        'year': 2015,
                        'product': 201,
                        'partner': 724,
                        'duty_type': 'AHS',
                        'simple_average': 5,
                        'number_of_total_lines': 13,
                    },
                    {
                        'reporter': 12,
                        'year': 2016,
                        'product': 201,
                        'partner': 710,
                        'duty_type': 'AHS',
                        'simple_average': 10,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 12,
                        'year': 2016,
                        'product': 201,
                        'partner': 705,
                        'duty_type': 'AHS',
                        'simple_average': 15,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 12,
                        'year': 2015,
                        'product': 201,
                        'partner': 0,
                        'duty_type': 'MFN',
                        'simple_average': 20,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 12,
                        'year': 2016,
                        'product': 201,
                        'partner': 0,
                        'duty_type': 'MFN',
                        'simple_average': 30,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 705,
                        'year': 2016,
                        'product': 201,
                        'partner': 0,
                        'duty_type': 'MFN',
                        'simple_average': 40,
                        'number_of_total_lines': 2,
                    },
                    {
                        'reporter': 705,
                        'year': 2016,
                        'product': 201,
                        'partner': 724,
                        'duty_type': 'AHS',
                        'simple_average': 60,
                        'number_of_total_lines': 2,
                    },
                ],
                # bound tariffs
                [],
                # year range
                ('2015', '2017'),
                # required countries
                [('ESP', 724, True), ('DZA', 12, True), ('SVN', 705, True)],
                # products
                None,
                # expected transformed tariffs
                #   Result format:
                #     product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                #     eu_rep_rate, eu_part_rate, eu_eu_rate, world_average
                [
                    (201, 12, 705, 2015, 20, None, 20, None, None, None, None, 20),
                    # mfn rate from 12-0 expanded
                    (201, 12, 705, 2016, 15, 15, 30, None, None, 15, None, 35),
                    # ahs 12-705
                    (201, 12, 705, 2017, 15, None, None, None, None, None, None, None),
                    # ahs previous year
                    (201, 12, 724, 2015, 20, None, 20, None, None, None, None, 20),
                    # mfn rate 12-0 expanded
                    (201, 12, 724, 2016, 15, None, 30, None, None, 15, None, 35),
                    # eu_part_rate as avg from 12-705 ahs
                    (201, 12, 724, 2017, 15, None, None, None, None, None, None, None),
                    # eu_part_rate from previous year
                    (201, 705, 12, 2015, 20, None, None, None, None, None, None, 20),
                    # world avg from 2015 mfn: 12-0
                    (201, 705, 12, 2016, 40, None, 40, None, None, None, None, 35),
                    # mfn rate: 705-0
                    (201, 705, 12, 2017, 40, None, None, None, None, None, None, None),
                    # mfn rate from previous year
                    (201, 705, 724, 2015, 0, None, None, None, 5, None, 0, 20),
                    # eu-eu trumps all
                    (201, 705, 724, 2016, 0, 60, 40, None, None, 60, 0, 35),
                    # eu-eu trumps all
                    (201, 705, 724, 2017, 0, None, None, None, None, None, 0, None),
                    # eu-eu trumps all
                    (201, 724, 12, 2015, 20, None, None, None, None, None, None, 20),
                    # world avg from 2015 mfn: 12-0
                    (201, 724, 12, 2016, 35, None, None, None, None, None, None, 35),
                    # world avg from 2016 mfn: 12-0, 705-0
                    (201, 724, 12, 2017, None, None, None, None, None, None, None, None),
                    # nothing available
                    (201, 724, 705, 2015, 0, None, None, None, None, None, 0, 20),
                    # eu-eu trumps all
                    (201, 724, 705, 2016, 0, None, None, None, None, None, 0, 35),
                    # eu-eu trumps all
                    (201, 724, 705, 2017, 0, None, None, None, None, None, 0, None)
                    # eu-eu trumps all
                ],
            ),
        ),
    )
    def test_transform_of_datafile(
        self,
        raw_tariffs,
        bound_tariffs,
        year_range,
        required_countries,
        only_products,
        expected_rows,
        add_world_bank_raw_tariff,
        add_world_bank_bound_rates,
        mocker,
    ):
        patch_years(mocker, year_range)
        patch_required_countries(mocker, required_countries)
        add_world_bank_raw_tariff(raw_tariffs)
        add_world_bank_bound_rates(bound_tariffs)
        pipeline = WorldBankTariffTransformPipeline(self.dbi, force=True, products=only_products)
        pipeline.process()
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline)

    @pytest.mark.parametrize(
        'continue_transform,expected_rows',
        (
            (True, PRODUCT_201_ROWS + PRODUCT_301_ROWS + PRODUCT_401_ROWS),
            (False, PRODUCT_301_ROWS + PRODUCT_401_ROWS),
        ),
    )
    def test_transform_of_datafile_continue(
        self, continue_transform, expected_rows, mocker, add_dit_baci
    ):
        patch_years(
            mocker, ('2018', '2018'),
        )
        patch_required_countries(
            mocker,
            countries=[
                ('BRA', 76, True),
                ('ZAF', 710, True),
                ('DZA', 12, True),
                ('AGO', 24, True),
                ('AUS', 36, True),
            ],
        )

        self.partial_transform_data()
        with mock.patch(
            'app.etl.organisation.world_bank.WorldBankTariffTransformPipeline._get_products'
        ) as mock_get_products:
            mock_get_products.return_value = [['301'], ['401']]
            pipeline = WorldBankTariffTransformPipeline(
                self.dbi, force=False, continue_transform=continue_transform
            )
            pipeline.process()
            assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline,)

    def partial_transform_data(self):
        pipeline = WorldBankTariffPipeline(self.dbi, force=True)
        fi = FileInfo.from_path(country_to_country_three_products)
        pipeline.process(fi)

        with mock.patch(
            'app.etl.organisation.world_bank.WorldBankTariffTransformPipeline._get_products'
        ) as mock_get_products:
            mock_get_products.return_value = [['201']]
            with mock.patch(
                'app.etl.organisation.world_bank.WorldBankTariffTransformPipeline'
                '.finish_processing'
            ) as mock_finish_processing:
                mock_finish_processing.return_value = None

                pipeline = WorldBankTariffTransformPipeline(
                    self.dbi, force=False, continue_transform=True
                )
                pipeline.process()
                assert rows_equal_table(
                    self.dbi, PRODUCT_201_ROWS, pipeline._l1_temp_table, pipeline
                )
                assert rows_equal_table(self.dbi, [], pipeline._l1_table, pipeline)

    @pytest.mark.parametrize(
        'continue_transform,products,expected_where_clause',
        (
            (False, None, ''),
            (
                True,
                None,
                'where product not in (select distinct product from "world_bank.tariff"."L1.temp")',
            ),
            (
                True,
                'hello,1234',
                'where product not in (select distinct product from "world_bank.tariff"."L1.temp")',
            ),
            (False, '1234', 'where product = 1234'),
            (False, '1234,5678,90', "where product in ('1234', '5678', '90')"),
            (False, 'Hello', ''),
            (
                True,
                '1234',
                (
                    'where product not in (select distinct product from '
                    '"world_bank.tariff"."L1.temp") and product = 1234'
                ),
            ),
            (
                True,
                '1234,5678,90',
                (
                    'where product not in '
                    '(select distinct product from "world_bank.tariff"."L1.temp") '
                    "and product in ('1234', '5678', '90')"
                ),
            ),
        ),
    )
    def test_product_where_clause(self, continue_transform, products, expected_where_clause):
        pipeline = WorldBankTariffTransformPipeline(
            self.dbi, force=False, continue_transform=continue_transform, products=products
        )
        result = pipeline.get_where_products_clause()
        assert result == expected_where_clause


def patch_required_countries(mocker, countries):
    def _create_required_countries_view_patched(self):
        stmt = f"""
        create materialized view if not exists {self._fq(self.required_countries_vn)} as (
            with required_countries (iso3, iso_num,	requirement) as (values
                {','.join([f"('{iso3}', {iso_num}, {req})"
                    for iso3, iso_num, req in countries])}
            ) select
                iso3,
                iso_num,
                requirement
            from required_countries
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    mocker.patch.object(
        WorldBankTariffTransformPipeline,
        '_create_required_countries_view',
        _create_required_countries_view_patched,
    )


def patch_years(mocker, year_range):
    mocker.patch.object(
        WorldBankTariffTransformPipeline, 'cutoff_year', year_range[0],
    )
    mocker.patch.object(
        WorldBankTariffTransformPipeline, 'final_year', year_range[1],
    )
