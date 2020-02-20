import pytest

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
