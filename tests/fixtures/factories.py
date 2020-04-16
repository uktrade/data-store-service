import factory
from flask import current_app as app

from app.db.models.external import (
    SPIRECountryGroup,
    SPIRERefCountryMapping,
)


class BaseFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        abstract = True
        sqlalchemy_session = app.db.session


class SPIRECountryGroupFactory(BaseFactory):
    class Meta:
        model = SPIRECountryGroup


class SPIRERefCountryMappingFactory(BaseFactory):
    country_name = factory.Faker('word')

    class Meta:
        model = SPIRERefCountryMapping

    @factory.post_generation
    def country_groups(self, create, extracted, **kwargs):
        if not create:
            return

        if extracted:
            for country_group in extracted:
                self.country_groups.append(country_group)
