import pytest

from app.db.models.external import Postcode


@pytest.fixture(scope='module')
def add_ons_postcode(app):
    def _method(records):
        for record in records:
            defaults = {
                'pcds': record.get('postcode'),
            }
            Postcode.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method
