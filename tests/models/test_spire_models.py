import pytest
from sqlalchemy.exc import IntegrityError

from tests.fixtures.factories import SPIREApplicationFactory


def test_application_fk_constraint(app_with_db):
    with pytest.raises(IntegrityError):
        SPIREApplicationFactory(batch=None)


def test_application_check_constraint_1(app_with_db):
    with pytest.raises(IntegrityError):
        SPIREApplicationFactory(case_type='HELLO')
