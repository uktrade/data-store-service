import pytest

from app.db.models.external import (
    ComtradeCountryCodeAndISOL1,
    DITEUCountryMembershipL1,
    DITReferencePostcodesL1,
    WorldBankTariffL1,
)
from app.db.models.external import ONSPostcodeDirectoryL1


@pytest.fixture(scope='module')
def add_ons_postcode(app):
    def _method(records):
        for record in records:
            defaults = {
                'pcds': record.get('postcode'),
            }
            ONSPostcodeDirectoryL1.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_dit_reference_postcodes(app):
    def _method(records):
        for record in records:
            defaults = {
                'postcode': record.get('postcode'),
                'local_authority_district_code': record.get('local_authority_district_code'),
            }
            DITReferencePostcodesL1.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_comtrade_country_code_and_iso(app):
    def _method(records):
        for record in records:
            defaults = {
                'data_source_row_id': record.get('data_source_row_id'),
                'cty_code': record.get('cty_code'),
                'cty_name_english': record.get('cty_name_english'),
                'cty_fullname_english': record.get('cty_fullname_english'),
                'cty_abbreviation': record.get('cty_abbreviation'),
                'cty_comments': record.get('cty_comments'),
                'iso2_digit_alpha': record.get('iso2_digit_alpha'),
                'iso3_digit_alpha': record.get('iso3_digit_alpha'),
                'start_valid_year': record.get('start_valid_year'),
                'end_valid_year': record.get('end_valid_year'),
            }
            ComtradeCountryCodeAndISOL1.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_dit_eu_country_membership(app):
    def _method(records):
        for record in records:
            defaults = {
                'data_source_row_id': record.get('data_source_row_id'),
                'country': record.get('country'),
                'iso3': record.get('iso3'),
                'year': record.get('year'),
                'tariff_code': record.get('tariff_code'),
            }
            DITEUCountryMembershipL1.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_world_bank_tariff(app):
    def _method(records):
        for record in records:
            defaults = {
                'data_source_row_id': record.get('data_source_row_id'),
                'product': record.get('product'),
                'reporter': record.get('reporter'),
                'partner': record.get('partner'),
                'year': record.get('year'),
                'assumed_tariff': record.get('assumed_tariff'),
                'app_rate': record.get('app_rate'),
                'mfn_rate': record.get('mfn_rate'),
                'prf_rate': record.get('prf_rate'),
                'bnd_rate': record.get('bnd_rate'),
                'country_average': record.get('country_average'),
                'world_average': record.get('world_average'),
            }
            WorldBankTariffL1.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method
