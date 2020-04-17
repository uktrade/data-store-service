import factory
from flask import current_app as app

from app.db.models.external import (
    SPIRECountryGroup,
    SPIRERefCountryMapping,
    SPIRECountryGroupEntry,
)


class BaseFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        abstract = True
        sqlalchemy_session = app.db.session


class SPIRECountryGroupFactory(BaseFactory):
    class Meta:
        model = SPIRECountryGroup


class SPIRECountryGroupEntryFactory(BaseFactory):
    country_group = factory.SubFactory(SPIRECountryGroupFactory)

    @factory.lazy_attribute
    def country_id(self):
        last = SPIRECountryGroupEntry.query.order_by(
            SPIRECountryGroupEntry.country_id.desc()
        ).first()
        if last:
            return last.country_id + 1
        return 1

    class Meta:
        model = SPIRECountryGroupEntry


class SPIRERefCountryMappingFactory(BaseFactory):
    country_name = factory.Faker('word')

    class Meta:
        model = SPIRERefCountryMapping
