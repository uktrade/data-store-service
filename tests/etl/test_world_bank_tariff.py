from unittest import mock

import pytest
from data_engineering.common.tests.conftest import create_tables
from datatools.io.fileinfo import FileInfo

from app.etl.organisation.world_bank import (
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline
)
from tests.utils import rows_equal_table

fixture_path = 'tests/fixtures/world_bank'

file_1 = f'{fixture_path}/tariff.csv'
eu_country_to_eu_country_file = f'{fixture_path}/eu_country_to_eu_country.csv'
eu_to_country_file = f'{fixture_path}/eu_to_country.csv'
country_to_world_file = f'{fixture_path}/country_to_world.csv'
countries_expanding_file = f'{fixture_path}/countries_expanding.csv'
country_to_country_two_years = f'{fixture_path}/country_to_country_two_years.csv'
country_to_country_three_products = f'{fixture_path}/country_to_country_three_products.csv'
forward_fill = f'{fixture_path}/forward_fill.csv'


PRODUCT_201_ROWS = [
    (201, 'AGO', 'BRA', 2018, 21.0, None, None, None, None, None, None, 21.0),
    (201, 'AGO', 'ZAF', 2018, 21.0, None, None, None, None, None, None, 21.0),
    (201, 'AUS', 'BRA', 2018, 21.0, None, None, None, None, None, None, 21.0),
    (201, 'AUS', 'ZAF', 2018, 21.0, None, None, None, None, None, None, 21.0),
    (201, 'DZA', 'BRA', 2018, 21.0, 21.0, None, None, None, None, 21.0, 21.0),
    (201, 'DZA', 'ZAF', 2018, 21.0, None, None, None, None, None, 21.0, 21.0),
]

PRODUCT_301_ROWS = [
    (301, 'AGO', 'BRA', 2018, 10.0, None, None, None, None, None, 10.0, 10.0),
    (301, 'AGO', 'ZAF', 2018, 10.0, 10.0, None, None, None, None, 10.0, 10.0),
    (301, 'AUS', 'BRA', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (301, 'AUS', 'ZAF', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (301, 'DZA', 'BRA', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (301, 'DZA', 'ZAF', 2018, 10.0, None, None, None, None, None, None, 10.0),
]

PRODUCT_401_ROWS = [
    (401, 'AGO', 'BRA', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (401, 'AGO', 'ZAF', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (401, 'AUS', 'BRA', 2018, 10.0, None, None, None, None, None, 10.0, 10.0),
    (401, 'AUS', 'ZAF', 2018, 10.0, 10.0, None, None, None, None, 10.0, 10.0),
    (401, 'DZA', 'BRA', 2018, 10.0, None, None, None, None, None, None, 10.0),
    (401, 'DZA', 'ZAF', 2018, 10.0, None, None, None, None, None, None, 10.0),
]

comtrade_countries = [
    {'id': 1, 'cty_code': 0, 'cty_name_english': 'World', 'iso3_digit_alpha': 'WLD'},
    {'id': 2, 'cty_code': 48, 'cty_name_english': 'Bahrain', 'iso3_digit_alpha': 'BHR'},
    {'id': 3, 'cty_code': 262, 'cty_name_english': 'Djibouti', 'iso3_digit_alpha': 'DJI'},
    {'id': 4, 'cty_code': 266, 'cty_name_english': 'Gabon', 'iso3_digit_alpha': 'GAB'},
    {'id': 5, 'cty_code': 381, 'cty_name_english': 'Italy', 'iso3_digit_alpha': 'ITA'},
    {'id': 6, 'cty_code': 918, 'cty_name_english': 'European Union', 'iso3_digit_alpha': 'EU'},
    {'id': 7, 'cty_code': 36, 'cty_name_english': 'Australia', 'iso3_digit_alpha': 'AUS'},
    {'id': 8, 'cty_code': 705, 'cty_name_english': 'Slovenia', 'iso3_digit_alpha': 'SVN'},
    {'id': 9, 'cty_code': 724, 'cty_name_english': 'Spain', 'iso3_digit_alpha': 'ESP'},
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

bound_rates = [{'reporter': 704, 'product': 201, 'bound_rate': 83.03}]


class TestWorldBankTariffPipeline:
    @pytest.fixture(autouse=True, scope='function')
    def setup(
        self,
        app_with_db,
        add_comtrade_country_code_and_iso,
        add_dit_eu_country_membership,
        add_world_bank_bound_rates,
    ):
        self.dbi = app_with_db.dbi
        self.dbi.execute_statement('drop table if exists "dit.baci"."L1" cascade;')
        create_tables(app_with_db)
        add_comtrade_country_code_and_iso(comtrade_countries)
        add_dit_eu_country_membership(eu_country_memberships)
        add_world_bank_bound_rates(bound_rates)

    def test_pipeline(self, add_dit_baci):
        add_dit_baci(
            [
                {
                    'id': 1,
                    'year': 2018,
                    'product_category': 201,
                    'exporter': 381,
                    'importer': 705,
                    'trade_flow_value': 0,
                    'quantity': 0,
                }
            ]
        )
        pipeline = WorldBankTariffPipeline(self.dbi, force=True)
        fi = FileInfo.from_path(file_1)
        pipeline.process(fi)

        # check L0
        expected_rows = [
            (48, 1999, 201, 0, 'AHS', 5.0, 20),
            (262, 2005, 201, 380, 'BND', 40.0, 40),
            (266, 1998, 201, 0, 'AHS', 20.0, 20),
        ]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        pipeline = WorldBankTariffTransformPipeline(self.dbi, force=True)
        pipeline.process()
        expected_rows = [(201, 'SVN', 'ITA', 2018, None, None, None, None, None, None, None, None)]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline)

        # check second run with different baci updates L1
        add_dit_baci(
            [
                {
                    'id': 2,
                    'year': 2018,
                    'product_category': 201,
                    'exporter': 36,
                    'importer': 705,
                    'trade_flow_value': 0,
                    'quantity': 0,
                }
            ]
        )
        pipeline.process()
        expected_rows = [
            (201, 'SVN', 'AUS', 2018, None, None, None, None, None, None, None, None),
            (201, 'SVN', 'ITA', 2018, None, None, None, None, None, None, None, None),
        ]
        assert rows_equal_table(self.dbi, expected_rows, pipeline._l1_table, pipeline)

    @pytest.mark.parametrize(
        'file_name,baci,required_countries,only_products,expected_rows',
        (
            (
                eu_country_to_eu_country_file,
                [
                    {
                        'id': 1,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 381,
                        'importer': 705,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    }
                ],  # i = partner, j = reporter
                [('SVN', 705, True), ('ITA', 381, True), ('ITA', 380, True)],
                None,
                # Result format:
                # product, reporter, partner, year, assumed_tariff, app_rate, mfn_rate, bnd_rate
                # eu_rep_rate, eu_part_rate, country_average, world_average
                [
                    (201, 'SVN', 'ITA', 2018, 76.51, 76.51, None, None, None, None, 76.51, 76.51)
                    # Italy has incorrect id 380 in product file and has to be fixed by the
                    # cleaning process and updated to 381
                ],
            ),
            (
                eu_to_country_file,
                [
                    {
                        'id': 1,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 36,  # partner: AUS
                        'importer': 705,  # reporter: SVN
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 2,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 36,  # partner: AUS
                        'importer': 724,  # reporter: ESP
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 3,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 705,  # partner: SVN
                        'importer': 724,  # reporter: ESP
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                ],
                [('SVN', 705, True), ('AUS', 36, True), ('ESP', 724, True)],
                None,
                #  When the reporter is EU (918) it's reporter_rate needs to be expanded
                #  to all required eu countries (SVN, ESP)
                [
                    (201, 'ESP', 'AUS', 2017, 81.96, None, None, None, 81.96, None, None, 20),
                    # eu_rep_rate expanded from 918-AUS
                    # eu_part_rate None because no EU-EU app rates available
                    # eu_rep_rate has precendence on world_average
                    # (derived rates are not part of country average)
                    (201, 'ESP', 'SVN', 2017, 0.0, None, None, None, 0.0, None, None, 20.0),
                    # EU - EU has zero rate
                    (201, 'SVN', 'AUS', 2017, 20, 20, None, None, 81.96, None, 20, 20),
                    # app has precedence on eu_rep_rate, eu_part_rate and world_average
                    # (201, 'SVN', 'ESP', 2017, 0.0, None, None, None, 0.0, None, 20.0, 20.0),
                    # excluded because not in baci spine
                ],
            ),
            (
                country_to_world_file,
                [
                    {
                        'id': 1,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 2,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 3,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 4,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                ],
                [('BRA', 76, True), ('ZAF', 710, True), ('DZA', 12, True), ('AGO', 24, True)],
                None,
                # Angola (24 - AGO) has a tariff specified with World & South Africa (710 - ZAF)
                # Algeria (12 - DZA) has a tariff specified with World & Brazil (76 - BRA)
                [
                    (201, 'AGO', 'BRA', 2017, 5.0, None, 5.0, None, None, None, 10.0, 15.5),
                    # mfn_rate used from AGO - WLD
                    # (201, 'AGO', 'DZA', 2017, 5.0, None, 5.0, None, None, None, 10.0, 15.5),
                    # excluded because not present in baci spine
                    (201, 'AGO', 'ZAF', 2017, 10.0, 10.0, 5.0, None, None, None, 10.0, 15.5),
                    # app rate has precedence on mfn_rate
                    # (201, 'DZA', 'AGO', 2017, 30.0, None, 30.0, None, None, None, 21.0, 15.5),
                    # excluded because not present in baci spine
                    (201, 'DZA', 'BRA', 2017, 21.0, 21.0, 30.0, None, None, None, 21.0, 15.5),
                    # app rate has precedence on mfn_rate
                    (201, 'DZA', 'ZAF', 2017, 30.0, None, 30.0, None, None, None, 21.0, 15.5),
                    # expanded to include required country and mfn_rate used from  AGO - WLD
                ],
            ),
            (
                country_to_country_two_years,
                [
                    {
                        'id': 1,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 2,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 3,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 4,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 5,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 6,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 7,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 8,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                ],
                [('BRA', 76, True), ('ZAF', 710, True), ('DZA', 12, True), ('AGO', 24, True)],
                None,
                [
                    (201, 'AGO', 'BRA', 2017, 10.0, None, None, None, None, None, 10.0, 15.5),
                    # country average has precedence
                    (201, 'AGO', 'BRA', 2018, 10.0, None, None, None, None, None, 10.0, 15.5),
                    (201, 'AGO', 'ZAF', 2017, 10.0, 10.0, None, None, None, None, 10.0, 15.5),
                    # app rate has precedence
                    (201, 'AGO', 'ZAF', 2018, 10.0, None, None, None, None, None, 10.0, 15.5),
                    (201, 'DZA', 'BRA', 2017, 21.0, None, None, None, None, None, 21.0, 15.5),
                    (201, 'DZA', 'BRA', 2018, 21.0, 21.0, None, None, None, None, 21.0, 15.5),
                    (201, 'DZA', 'ZAF', 2017, 21.0, None, None, None, None, None, 21.0, 15.5),
                    (201, 'DZA', 'ZAF', 2018, 21.0, None, None, None, None, None, 21.0, 15.5),
                ],
            ),
            (
                countries_expanding_file,
                [
                    {
                        'id': 1,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 2,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 3,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 4,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 5,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 36,
                        'importer': 704,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                ],
                [
                    ('BRA', 76, True),
                    ('ZAF', 710, True),
                    ('DZA', 12, True),
                    ('AGO', 24, True),
                    ('VNM', 704, True),
                    ('AUS', 36, True),
                ],
                None,
                # Same as country_to_world file with World entries removed and a
                # tariff between Australia (36) to Vietnam (704) and a BND rate
                # added for vietnam, different world average
                [
                    (201, 'AGO', 'AUS', 2017, 10.0, None, None, None, None, None, 10.0, 16.333),
                    (201, 'AGO', 'BRA', 2017, 10.0, None, None, None, None, None, 10.0, 16.333),
                    (201, 'AGO', 'ZAF', 2017, 10.0, 10.0, None, None, None, None, 10.0, 16.333),
                    (201, 'DZA', 'AUS', 2017, 21.0, None, None, None, None, None, 21.0, 16.333),
                    (201, 'DZA', 'BRA', 2017, 21.0, 21.0, None, None, None, None, 21.0, 16.333),
                    (201, 'DZA', 'ZAF', 2017, 21.0, None, None, None, None, None, 21.0, 16.333),
                    (201, 'VNM', 'AUS', 2017, 18.0, 18.0, None, 83.03, None, None, 18.0, 16.333),
                    (201, 'VNM', 'BRA', 2017, 18.0, None, None, 83.03, None, None, 18.0, 16.333),
                    (201, 'VNM', 'ZAF', 2017, 18.0, None, None, 83.03, None, None, 18.0, 16.333),
                ],
            ),
            (
                country_to_country_three_products,
                [
                    {
                        'id': 1,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                    {
                        'id': 2,
                        'year': 2018,
                        'product_category': 301,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                    {
                        'id': 3,
                        'year': 2018,
                        'product_category': 401,
                        'exporter': 710,
                        'importer': 36,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                ],
                [
                    ('BRA', 76, True),
                    ('ZAF', 710, True),
                    ('DZA', 12, True),
                    ('AGO', 24, True),
                    ('AUS', 36, True),
                ],
                None,
                PRODUCT_201_ROWS + PRODUCT_301_ROWS + PRODUCT_401_ROWS,
            ),
            (
                country_to_country_three_products,
                [
                    {
                        'id': 1,
                        'year': 2018,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                    {
                        'id': 2,
                        'year': 2018,
                        'product_category': 301,
                        'exporter': 710,
                        'importer': 24,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                    {
                        'id': 3,
                        'year': 2018,
                        'product_category': 401,
                        'exporter': 710,
                        'importer': 36,
                        'trade_flow_value': 50,
                        'quantity': 20,
                    },
                ],
                [
                    ('BRA', 76, True),
                    ('ZAF', 710, True),
                    ('DZA', 12, True),
                    ('AGO', 24, True),
                    ('AUS', 36, True),
                ],
                '201,401',
                PRODUCT_201_ROWS + PRODUCT_401_ROWS,
            ),
            (
                forward_fill,
                [
                    {
                        'id': 1,
                        'year': 2015,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 2,
                        'year': 2016,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 3,
                        'year': 2016,
                        'product_category': 201,
                        'exporter': 710,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                    {
                        'id': 4,
                        'year': 2017,
                        'product_category': 201,
                        'exporter': 76,
                        'importer': 12,
                        'trade_flow_value': 0,
                        'quantity': 0,
                    },
                ],
                [('BRA', 76, True), ('DZA', 12, True), ('ZAF', 710, True)],
                None,
                [
                    (201, 'DZA', 'BRA', 2015, 21.0, 21.0, None, None, None, None, 15.5, 15.5),
                    (201, 'DZA', 'BRA', 2016, 21.0, None, None, None, None, None, 15.5, 15.5),
                    # take previous rate for missing year
                    (201, 'DZA', 'BRA', 2017, 21.0, None, None, None, None, None, 15.5, 15.5),
                    (201, 'DZA', 'ZAF', 2015, 15.5, None, None, None, None, None, 15.5, 15.5),
                    (201, 'DZA', 'ZAF', 2016, 10.0, 10.0, None, None, None, None, 15.5, 15.5),
                    (201, 'DZA', 'ZAF', 2017, 10.0, None, None, None, None, None, 15.5, 15.5),
                ],
            ),
        ),
    )
    def test_transform_of_datafile(
        self,
        file_name,
        baci,
        required_countries,
        only_products,
        expected_rows,
        add_dit_baci,
        mocker,
    ):
        add_dit_baci(baci)
        patch_required_countries(mocker, required_countries)
        pipeline = WorldBankTariffPipeline(self.dbi, force=True)
        fi = FileInfo.from_path(file_name)
        pipeline.process(fi)
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
        add_dit_baci(
            [
                {
                    'id': 1,
                    'year': 2018,
                    'product_category': 201,
                    'exporter': 76,
                    'importer': 12,
                    'trade_flow_value': 50,
                    'quantity': 20,
                },
                {
                    'id': 2,
                    'year': 2018,
                    'product_category': 301,
                    'exporter': 710,
                    'importer': 24,
                    'trade_flow_value': 50,
                    'quantity': 20,
                },
                {
                    'id': 3,
                    'year': 2018,
                    'product_category': 401,
                    'exporter': 710,
                    'importer': 36,
                    'trade_flow_value': 50,
                    'quantity': 20,
                },
            ]
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
                'app.etl.organisation.world_bank.WorldBankTariffTransformPipeline.finish_processing'
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
