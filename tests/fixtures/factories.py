import factory
from flask import current_app as app

from app.db.models.external import (
    SPIREApplication,
    SPIREApplicationAmendment,
    SPIREApplicationCountry,
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


class SPIREBatchFactory(BaseFactory):
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    end_date = factory.Faker('date_time_between', start_date='-1y')
    batch_ref = factory.Faker('random_int', min=1, max=50)

    status = factory.Faker('random_element', elements=('RELEASED',))

    @factory.lazy_attribute
    def approve_date(self):
        return factory.Faker(
            'date_time_between', start_date=self.start_date, end_date=self.end_date
        ).generate({})

    @factory.lazy_attribute
    def release_date(self):
        return factory.Faker(
            'date_time_between', start_date=self.staging_date, end_date=self.end_date
        ).generate({})

    @factory.lazy_attribute
    def staging_date(self):
        return factory.Faker(
            'date_time_between', start_date=self.approve_date, end_date=self.end_date
        ).generate({})

    class Meta:
        model = SPIREBatch


class SPIREApplicationFactory(BaseFactory):
    case_type = factory.Faker('safe_color_name')
    case_sub_type = factory.Faker('color_name')
    initial_processing_time = factory.Faker('random_int')
    case_closed_date = factory.Faker('date_this_century')
    withheld_status = factory.Faker('word')
    batch = factory.SubFactory(SPIREBatchFactory)
    ela_id = factory.Faker('random_int', min=1, max=50)

    class Meta:
        model = SPIREApplication


class SPIREApplicationCountryFactory(BaseFactory):
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    application = factory.SubFactory(SPIREApplicationFactory)
    batch = factory.SubFactory(SPIREBatchFactory)

    @factory.lazy_attribute
    def report_date(self):
        return factory.Faker('date_time_between', start_date=self.start_date,).generate({})

    @factory.lazy_attribute
    def country_id(self):
        last = SPIREApplicationCountry.query.order_by(
            SPIREApplicationCountry.country_id.desc()
        ).first()
        if last:
            return last.country_id + 1
        return 1

    class Meta:
        model = SPIREApplicationCountry


class SPIREApplicationAmendmentFactory(BaseFactory):
    case_type = factory.Faker('safe_color_name')
    case_sub_type = factory.Faker('color_name')
    case_processing_time = factory.Faker('random_int')
    amendment_closed_date = factory.Faker('date_this_century')
    application = factory.SubFactory(SPIREApplicationFactory)
    withheld_status = factory.Faker('word')
    batch_id = factory.Faker('random_int', min=1, max=50)

    @factory.lazy_attribute
    def ela_id(self):
        last = SPIREApplicationAmendment.query.order_by(
            SPIREApplicationAmendment.ela_id.desc()
        ).first()
        if last:
            return last.ela_id + 1
        return 1

    class Meta:
        model = SPIREApplicationAmendment
