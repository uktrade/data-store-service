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
    SPIREEndUser,
    SPIREFootnote,
    SPIREFootnoteEntry,
    SPIREGoodsIncident,
    SPIREIncident,
    SPIREMediaFootnote,
    SPIREMediaFootnoteCountry,
    SPIREMediaFootnoteDetail,
    SPIREOglType,
    SPIREReasonForRefusal,
    SPIRERefArsSubject,
    SPIRERefCountryMapping,
    SPIRERefDoNotReportValue,
    SPIRERefReportRating,
    SPIREReturn,
    SPIREThirdParty,
    SPIREUltimateEndUser,
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
    inc_id = factory.Faker('random_element', elements=list(range(1, 200)))
    goods_item_id = factory.Faker('random_element', elements=list(range(1, 200)))
    dest_country_id = factory.Faker('random_element', elements=list(range(1, 200)))
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
    report_rating = factory.Faker('random_element', elements=['IRN', 'ML1', 'ML10', 'ML11'])

    class Meta:
        model = SPIRERefReportRating


class SPIREControlEntryFactory(BaseFactory):
    value = factory.Faker('pydecimal', positive=True, right_digits=2, left_digits=6)
    ref_report_rating = factory.SubFactory(SPIRERefReportRatingFactory)
    goods_incident = factory.SubFactory(SPIREGoodsIncidentFactory)

    class Meta:
        model = SPIREControlEntry


class SPIREEndUserFactory(BaseFactory):
    ela_grp_id = factory.Faker('random_number', digits=6, fix_len=True)
    end_user_type = factory.Faker('random_element', elements=['COM', 'IND', None, 'GOV', 'OTHER'])
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    version_number = factory.Faker('random_int', min=1, max=3)
    country_id = factory.Faker('random_int', min=1, max=200)
    end_user_count = factory.Faker('random_int', min=1, max=4)
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    batch_id = factory.Faker('random_int', min=1, max=50)

    @factory.lazy_attribute
    def eu_id(self):
        last = SPIREEndUser.query.order_by(SPIREEndUser.eu_id.desc()).first()
        if last:
            return last.eu_id + 1
        return 1

    class Meta:
        model = SPIREEndUser


class SPIREMediaFootnoteDetailFactory(BaseFactory):
    media_footnote = factory.SubFactory(SPIREMediaFootnoteFactory)
    start_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    footnote_type = factory.Faker('random_element', elements=['STANDARD', 'END_USER'])
    display_text = factory.Faker('sentence')
    single_footnote_text = factory.Faker('paragraph')
    joint_footnote_text = factory.Faker('paragraph')

    @factory.lazy_attribute
    def end_datetime(self):
        if not random.randint(0, 3):
            return factory.Faker('date_between', start_date=self.start_datetime).generate({})
        return

    class Meta:
        model = SPIREMediaFootnoteDetail


class SPIREFootnoteEntryFactory(BaseFactory):
    batch = factory.SubFactory(SPIREBatchFactory)
    footnote = factory.SubFactory(SPIREFootnoteFactory)
    application = factory.SubFactory(SPIREApplicationFactory)
    media_footnote_detail = factory.SubFactory(SPIREMediaFootnoteDetailFactory)

    goods_item_id = factory.Faker('random_int', min=1, max=200)
    country_id = factory.Faker('random_int', min=1, max=200)
    fnr_id = factory.Faker('random_int', min=500, max=2000)
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=1, max=3)
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    mf_grp_id = factory.Faker('random_element', elements=[1, 2, 3, None])
    mf_free_text = factory.Faker('sentence')

    @factory.lazy_attribute
    def fne_id(self):
        last = SPIREFootnoteEntry.query.order_by(SPIREFootnoteEntry.fne_id.desc()).first()
        if last:
            return last.fne_id + 1
        return 1

    class Meta:
        model = SPIREFootnoteEntry


class SPIREMediaFootnoteCountryFactory(BaseFactory):
    ela_grp_id = factory.Faker('random_number', digits=6, fix_len=True)
    mf_grp_id = factory.Faker('random_element', elements=[1, 2, 3])
    country_id = factory.Faker('random_int', min=1, max=200)
    country_name = factory.Faker('word')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    start_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')

    @factory.lazy_attribute
    def end_datetime(self):
        if not random.randint(0, 3):
            return factory.Faker('date_between', start_date=self.start_datetime).generate({})
        return

    class Meta:
        model = SPIREMediaFootnoteCountry


class SPIREIncidentFactory(BaseFactory):
    batch = factory.SubFactory(SPIREBatchFactory)
    status = factory.Faker('random_element', elements=['READY'])
    type = factory.Faker(
        'random_element',
        elements=[
            'ISSUE',
            'DEREGISTRATION',
            'REDUCTION',
            'REFUSAL',
            'REVOKE',
            'SURRENDER',
            'SUSPENSION',
        ],
    )
    case_type = factory.Faker(
        'random_element', elements=['SIEL', 'OGEL', 'OIEL', 'OITCL', 'GPL', 'SITCL', 'TA_OIEL']
    )
    case_sub_type = factory.Faker(
        'random_element',
        elements=[
            'CRYPTO',
            'MEDIA',
            'DEALER',
            'MIL_DUAL',
            None,
            'PERMANENT',
            'TEMPORARY',
            'TRANSHIPMENT',
            'UKCONTSHELF',
        ],
    )
    application = factory.SubFactory(SPIREApplicationFactory)
    report_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    version_no = factory.Faker('random_int', min=1, max=3)
    temporary_licence_flag = factory.Faker('random_int', min=0, max=1)
    licence_conversion_flag = factory.Faker('random_int', min=0, max=1)
    incorporation_flag = factory.Faker('random_int', min=0, max=1)
    mil_flag = factory.Faker('random_int', min=0, max=1)
    other_flag = factory.Faker('random_int', min=0, max=1)
    torture_flag = factory.Faker('random_int', min=0, max=1)
    licence_id = factory.Faker('random_int', min=1, max=99999)

    @factory.lazy_attribute
    def ogl_id(self):
        if not random.randint(0, 4):
            return factory.Faker('random_int', min=1, max=100).generate({})
        return

    @factory.lazy_attribute
    def else_id(self):
        if not random.randint(0, 4):
            return factory.Faker('random_int', min=1, max=100).generate({})
        return

    @factory.lazy_attribute
    def start_date(self):
        return factory.Faker('date_time_between', start_date=self.report_date).generate({})

    @factory.lazy_attribute
    def inc_id(self):
        last = SPIREIncident.query.order_by(SPIREIncident.inc_id.desc()).first()
        if last:
            return last.inc_id + 1
        return 1

    class Meta:
        model = SPIREIncident


class SPIREOglTypeFactory(BaseFactory):
    title = factory.Faker('sentence', nb_words=4)
    start_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    display_order = factory.Faker('random_int', min=100, max=999)
    f680_flag = factory.Faker('random_element', elements=['Y', 'N', None])

    @factory.lazy_attribute
    def end_datetime(self):
        if not random.randint(0, 3):
            return factory.Faker('date_between', start_date=self.start_datetime).generate({})
        return

    class Meta:
        model = SPIREOglType


class SPIREReturnFactory(BaseFactory):
    batch = factory.SubFactory(SPIREBatchFactory)
    elr_version = factory.Faker('random_int', min=1, max=50)
    end_user_type = factory.Faker('random_element', elements=['COM', 'IND', None, 'GOV', 'OTHER'])
    status = factory.Faker('random_element', elements=['ACTIVE', 'WITHDRAWN'])
    created_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    licence_type = factory.Faker(
        'random_element', elements=['SIEL', 'OGEL', 'OIEL', 'OITCL', 'GPL', 'SITCL', 'TA_OIEL']
    )
    el_id = factory.Faker('random_int', min=1, max=99999)
    usage_count = factory.Faker('random_int', min=0, max=1)

    @factory.lazy_attribute
    def elr_id(self):
        last = SPIREReturn.query.order_by(SPIREReturn.elr_id.desc()).first()
        if last:
            return last.elr_id + 1
        return 1

    class Meta:
        model = SPIREReturn


class SPIREThirdPartyFactory(BaseFactory):
    application = factory.SubFactory(SPIREApplicationFactory)
    batch = factory.SubFactory(SPIREBatchFactory)
    country_id = factory.Faker('random_int', min=1, max=200)
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=1, max=3)
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    ultimate_end_user_flag = factory.Faker('random_int', min=1, max=200)

    @factory.lazy_attribute
    def tp_id(self):
        last = SPIREThirdParty.query.order_by(SPIREThirdParty.tp_id.desc()).first()
        if last:
            return last.tp_id + 1
        return 1

    class Meta:
        model = SPIREThirdParty


class SPIREUltimateEndUserFactory(BaseFactory):
    application = factory.SubFactory(SPIREApplicationFactory)
    batch = factory.SubFactory(SPIREBatchFactory)
    country_id = factory.Faker('random_int', min=1, max=200)
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=1, max=3)

    @factory.lazy_attribute
    def ueu_id(self):
        last = SPIREUltimateEndUser.query.order_by(SPIREUltimateEndUser.ueu_id.desc()).first()
        if last:
            return last.ueu_id + 1
        return 1

    class Meta:
        model = SPIREUltimateEndUser
