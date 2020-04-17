import factory
from flask import current_app as app

from app.db.models.external import (
    SPIREApplication,
    SPIREBatch,
    SPIRECountryGroup,
    SPIRECountryGroupEntry,
    SPIRERefCountryMapping,
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


class SPIREApplicationFactory(BaseFactory):
    case_type = factory.Faker('safe_color_name')
    case_sub_type = factory.Faker('color_name')
    initial_processing_time = factory.Faker('random_int')
    case_closed_date = factory.Faker('date_this_century')
    withheld_status = factory.Faker('word')
    batch_id = None
    ela_id = factory.Faker('random_int', min=1, max=50)

    class Meta:
        model = SPIREApplication


class SPIREBatchFactory(BaseFactory):
    start_date = factory.Faker('date_between', start_date='-2y', end_date='-1y')
    end_date = factory.Faker('date_between', start_date='-1y')
    batch_ref = factory.Faker('random_int', min=1, max=50)

    status = factory.Faker(
        'random_element', elements=('RELEASED',)
    )

    @factory.lazy_attribute
    def approve_date(self):
        return factory.Faker(
            'date_between', start_date=self.start_date, end_date=self.end_date
        ).generate({})

    @factory.lazy_attribute
    def release_date(self):
        return factory.Faker(
            'date_between', start_date=self.staging_date, end_date=self.end_date
        ).generate({})

    @factory.lazy_attribute
    def staging_date(self):
        return factory.Faker(
            'date_between', start_date=self.approve_date, end_date=self.end_date
        ).generate({})

    class Meta:
        model = SPIREBatch
