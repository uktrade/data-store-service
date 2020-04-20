import random

import factory
from flask import current_app as app

from app.db.models.external import (
    SPIREApplication,
    SPIREApplicationAmendment,
    SPIREApplicationCountry,
    SPIREArs,
    SPIREBatch,
    SPIREControlEntry,
    SPIRECountryGroup,
    SPIRECountryGroupEntry,
    SPIREFootnote,
    SPIREGoodsIncident,
    SPIREMediaFootnote,
    SPIREReasonForRefusal,
    SPIRERefArsSubject,
    SPIRERefCountryMapping,
    SPIRERefDoNotReportValue,
    SPIRERefReportRating,
    SPIREEndUser,
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


class SPIREGoodsIncidentFactory(BaseFactory):
    start_date = factory.Faker('date_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    type = factory.Faker('random_element', elements=['ISSUE'])
    batch = factory.SubFactory(SPIREBatchFactory)
    version_no = factory.Faker('random_int', min=1, max=3)
    inc_id = factory.Faker('random_element', elements=[None] + list(range(1, 200)))
    goods_item_id = factory.Faker('random_element', elements=[None] + list(range(1, 200)))
    dest_country_id = factory.Faker('random_element', elements=[None] + list(range(1, 200)))
    application = factory.SubFactory(SPIREApplicationFactory)
    country_group = factory.SubFactory(SPIRECountryGroupFactory)

    @factory.lazy_attribute
    def report_date(self):
        return factory.Faker('date_between', start_date=self.start_date).generate({})

    class Meta:
        model = SPIREGoodsIncident


class SPIREArsFactory(BaseFactory):
    ars_value = factory.Faker('sentence', nb_words=4, variable_nb_words=True)
    ars_quantity = factory.Faker('random_element', elements=[None] + list(range(1, 20)))
    goods_incident = factory.SubFactory(SPIREGoodsIncidentFactory)

    class Meta:
        model = SPIREArs


class SPIREMediaFootnoteFactory(BaseFactory):
    class Meta:
        model = SPIREMediaFootnote


class SPIREFootnoteFactory(BaseFactory):
    status = factory.Faker('random_element', elements=['CURRENT', 'DELETED'])

    @factory.lazy_attribute
    def text(self):
        paragraphs = factory.Faker('paragraphs', nb=random.randint(0, 3)).generate({})
        return '\n'.join(paragraphs)

    class Meta:
        model = SPIREFootnote


class SPIRERefArsSubjectFactory(BaseFactory):
    ars_subject = factory.Faker('sentence', nb_words=2, variable_nb_words=False)
    ars_value = factory.Faker('sentence', nb_words=8)

    class Meta:
        model = SPIRERefArsSubject


class SPIRERefDoNotReportValueFactory(BaseFactory):
    dnr_type = factory.Faker('random_element', elements=['ARS_SUBJECT', 'CE'])
    dnr_value = factory.Faker('sentence', nb_words=8)

    class Meta:
        model = SPIRERefDoNotReportValue


class SPIREReasonForRefusalFactory(BaseFactory):
    goods_incident = factory.SubFactory(SPIREGoodsIncidentFactory)
    reason_for_refusal = factory.Faker('word')

    class Meta:
        model = SPIREReasonForRefusal


class SPIRERefReportRatingFactory(BaseFactory):
    rating = factory.Faker('sentence', nb_words=4)

    class Meta:
        model = SPIRERefReportRating


class SPIREControlEntryFactory(BaseFactory):
    value = factory.Faker('random_int', min=1, max=3)
    ref_report_rating = factory.SubFactory(SPIRERefReportRatingFactory)
    goods_incident = factory.SubFactory(SPIREGoodsIncidentFactory)

    class Meta:
        model = SPIREControlEntry


class SPIREEndUserFactory(BaseFactory):
    ela_grp_id = factory.Faker('random_number', digits=6, fix_len=True)
    end_user_type = factory.Faker('random_element', elements=['COM', 'IND', 'NULL', 'GOV', 'OTHER'])
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    version_number = factory.Faker('random_int', min=1, max=3)
    country_id = factory.Faker('random_int', min=1, max=200)
    end_user_count = factory.Faker('random_int', min=1, max=4)
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    batch_id = factory.Faker('random_int', min=1, max=50)

    @factory.lazy_attribute
    def eu_id(self):
        last = SPIREEndUser.query.order_by(
            SPIREEndUser.eu_id.desc()
        ).first()
        if last:
            return last.eu_id + 1
        return 1

    class Meta:
        model = SPIREEndUser
