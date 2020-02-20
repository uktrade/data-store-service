import pytest

from app.db.models.external import DITReferencePostcodesL1
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
