import random

import factory
from flask import current_app as app
from sqlalchemy.orm.scoping import scoped_session

from app.db.models.external import (
    BaseModel,
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
from app.db.models.internal import Pipeline, PipelineDataFile


def get_session():
    return app.db.session


def set_pk(model: BaseModel, field: str) -> int:
    order = getattr(model, field)
    last = model.query.order_by(order.desc()).first()
    if last:
        return getattr(last, field) + 1
    return 1


class BaseFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        abstract = True
        sqlalchemy_session = scoped_session(get_session)
        sqlalchemy_session_persistence = 'commit'


class SPIRECountryGroupFactory(BaseFactory):
    class Meta:
        model = SPIRECountryGroup


class SPIRECountryGroupEntryFactory(BaseFactory):
    country_group = factory.SubFactory(SPIRECountryGroupFactory)

    @factory.lazy_attribute
    def country_id(self):
        return set_pk(SPIRECountryGroupEntry, 'country_id')

    class Meta:
        model = SPIRECountryGroupEntry


class SPIRERefCountryMappingFactory(BaseFactory):
    country_name = factory.Faker('word')

    class Meta:
        model = SPIRERefCountryMapping


class SPIREBatchFactory(BaseFactory):
    approve_date = factory.Faker('date_time_between', start_date='-1y')
    status = factory.Faker('random_element', elements=['RELEASED', 'STAGING'])

    @factory.lazy_attribute
    def batch_ref(self):
        batch_ref = str(factory.Faker('random_int', min=1, max=50).generate({}))
        if not random.randint(0, 3):
            return f'C{batch_ref}'
        return batch_ref

    @factory.lazy_attribute
    def start_date(self):
        if self.batch_ref[0] != 'C':
            return factory.Faker('date_between', start_date='-2y', end_date='-1y').generate({})

    @factory.lazy_attribute
    def end_date(self):
        if self.batch_ref[0] != 'C' and self.start_date and self.approve_date:
            return factory.Faker(
                'date_between', start_date=self.start_date, end_date=self.approve_date
            ).generate({})

    @factory.lazy_attribute
    def release_date(self):
        if self.approve_date and self.end_date:
            return factory.Faker(
                'date_time_between',
                start_date=self.approve_date,
            ).generate({})

    @factory.lazy_attribute
    def staging_date(self):
        if self.approve_date and self.end_date:
            return factory.Faker(
                'date_time_between',
                start_date=self.approve_date,
            ).generate({})

    class Meta:
        model = SPIREBatch


class SPIREApplicationFactory(BaseFactory):
    case_type = factory.Faker(
        'random_element',
        elements=['SIEL', 'OGEL', 'OIEL', 'OITCL', 'GPL', 'SITCL', 'TA_OIEL', 'TA_SIEL'],
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
    initial_processing_time = factory.Faker('random_int')
    case_closed_date = factory.Faker('date_this_century')
    withheld_status = factory.Faker('random_element', elements=['PENDING', 'WITHHELD', None])
    batch = factory.SubFactory(SPIREBatchFactory)
    ela_id = factory.Faker('random_int', min=1, max=50)

    @factory.lazy_attribute
    def case_sub_type(self):
        if self.case_type == 'SIEL':
            return factory.Faker(
                'random_element',
                elements=['PERMANENT', 'TEMPORARY', 'TRANSHIPMENT'],
            ).generate({})
        if self.case_type == 'OIEL':
            return factory.Faker(
                'random_element',
                elements=['CRYPTO', 'MEDIA', 'DEALER', 'MIL_DUAL', 'UKCONTSHELF'],
            ).generate({})

    class Meta:
        model = SPIREApplication


class SPIREApplicationCountryFactory(BaseFactory):
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    batch = factory.SubFactory(SPIREBatchFactory)
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )

    @factory.lazy_attribute
    def report_date(self):
        return factory.Faker(
            'date_time_between',
            start_date=self.start_date,
        ).generate({})

    @factory.lazy_attribute
    def country_id(self):
        return set_pk(SPIREApplicationCountry, 'country_id')

    class Meta:
        model = SPIREApplicationCountry


class SPIREApplicationAmendmentFactory(BaseFactory):
    case_type = factory.Faker(
        'random_element',
        elements=['SIEL', 'OGEL', 'OIEL', 'OITCL', 'GPL', 'SITCL', 'TA_OIEL', 'TA_SIEL'],
    )
    case_sub_type = factory.Faker('color_name')
    case_processing_time = factory.Faker('random_int')
    amendment_closed_date = factory.Faker('date_this_century')
    application = factory.SubFactory(SPIREApplicationFactory)
    withheld_status = factory.Faker('random_element', elements=['PENDING', 'WITHHELD'])
    batch_id = factory.Faker('random_int', min=1, max=50)

    @factory.lazy_attribute
    def ela_id(self):
        return set_pk(SPIREApplicationAmendment, 'ela_id')

    class Meta:
        model = SPIREApplicationAmendment


class SPIREGoodsIncidentFactory(BaseFactory):
    start_date = factory.Faker('date_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    type = factory.Faker('random_element', elements=['ISSUE', 'REFUSAL', 'REVOKE', 'SURRENDER'])
    batch = factory.SubFactory(SPIREBatchFactory)
    version_no = factory.Faker('random_int', min=0, max=3)
    inc_id = factory.Faker('random_element', elements=list(range(1, 200)))
    goods_item_id = factory.Faker('random_element', elements=list(range(1, 200)))
    dest_country_id = factory.Faker('random_element', elements=list(range(1, 200)))
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )
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
    status = factory.Faker('random_element', elements=['CURRENT', 'DELETED', 'ARCHIVED'])

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
    reason_for_refusal = factory.Faker('sentence', nb_words=8, variable_nb_words=False)

    class Meta:
        model = SPIREReasonForRefusal


class SPIRERefReportRatingFactory(BaseFactory):
    rating = factory.Faker('sentence', nb_words=8, variable_nb_words=False)
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
        return set_pk(SPIREEndUser, 'eu_id')

    class Meta:
        model = SPIREEndUser


class SPIREMediaFootnoteDetailFactory(BaseFactory):
    media_footnote = factory.SubFactory(SPIREMediaFootnoteFactory)
    start_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=[None, 'C'])
    footnote_type = factory.Faker('random_element', elements=['STANDARD', 'END_USER'])
    display_text = factory.Faker('sentence')
    single_footnote_text = factory.Faker('paragraph')
    joint_footnote_text = factory.Faker('paragraph')

    @factory.lazy_attribute
    def end_datetime(self):
        if self.status_control == 'C':
            return
        if not self.status_control:
            return factory.Faker('date_between', start_date=self.start_datetime).generate({})

    class Meta:
        model = SPIREMediaFootnoteDetail


class SPIREFootnoteEntryFactory(BaseFactory):
    batch = factory.SubFactory(SPIREBatchFactory)
    footnote = factory.SubFactory(SPIREFootnoteFactory)
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )

    media_footnote_detail = None

    goods_item_id = factory.Faker('random_int', min=1, max=200)
    country_id = None
    fnr_id = None
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=1, max=3)
    status_control = factory.Faker('random_element', elements=['A', 'C'])

    @factory.lazy_attribute
    def fne_id(self):
        return set_pk(SPIREFootnoteEntry, 'fne_id')

    @factory.lazy_attribute
    def mf_free_text(self):
        if self.footnote:
            return
        if self.media_footnote_detail:
            return
        return factory.Faker('sentence').generate({})

    @factory.lazy_attribute
    def mf_grp_id(self):
        if self.footnote:
            return
        if self.mf_free_text or self.media_footnote_detail:
            return factory.Faker('random_element', elements=[1, 2, 3]).generate({})

    class Meta:
        model = SPIREFootnoteEntry


class SPIREMediaFootnoteCountryFactory(BaseFactory):
    ela_grp_id = factory.Faker('random_number', digits=6, fix_len=True)
    mf_grp_id = factory.Faker('random_element', elements=[1, 2, 3])
    country_id = factory.Faker('random_int', min=1, max=200)
    country_name = factory.Faker('word')
    status_control = factory.Faker('random_element', elements=['C', None])
    start_datetime = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')

    @factory.lazy_attribute
    def end_datetime(self):
        if self.status_control == 'C':
            return
        return factory.Faker('date_between', start_date=self.start_datetime).generate({})

    class Meta:
        model = SPIREMediaFootnoteCountry


class SPIREIncidentFactory(BaseFactory):
    batch = factory.SubFactory(SPIREBatchFactory)
    status = factory.Faker('random_element', elements=['READY', 'FOR_ATTENTION'])
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
        'random_element',
        elements=['SIEL', 'OGEL', 'OIEL', 'OITCL', 'GPL', 'SITCL', 'TA_OIEL', 'TA_SIEL'],
    )
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )
    report_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    status_control = factory.Faker('random_element', elements=['A', 'C'])
    version_no = factory.Faker('random_int', min=0, max=3)
    temporary_licence_flag = factory.Faker('random_int', min=0, max=1)
    licence_conversion_flag = factory.Faker('random_int', min=0, max=1)
    incorporation_flag = factory.Faker('random_int', min=0, max=1)
    mil_flag = factory.Faker('random_int', min=0, max=1)
    other_flag = factory.Faker('random_int', min=0, max=1)
    torture_flag = factory.Faker('random_int', min=0, max=1)

    @factory.lazy_attribute
    def ogl_id(self):
        if self.case_type == 'OGEL':
            return factory.Faker('random_int', min=1, max=100).generate({})
        return

    @factory.lazy_attribute
    def else_id(self):
        if self.type == 'SUSPENSION':
            return factory.Faker('random_int', min=1, max=100).generate({})
        return

    @factory.lazy_attribute
    def licence_id(self):
        if self.type != 'REFUSAL':
            return factory.Faker('random_int', min=1, max=99999).generate({})
        return

    @factory.lazy_attribute
    def start_date(self):
        return factory.Faker('date_time_between', start_date=self.report_date).generate({})

    @factory.lazy_attribute
    def inc_id(self):
        return set_pk(SPIREIncident, 'inc_id')

    @factory.lazy_attribute
    def case_sub_type(self):
        if self.case_type == 'SIEL':
            return factory.Faker(
                'random_element',
                elements=['PERMANENT', 'TEMPORARY', 'TRANSHIPMENT'],
            ).generate({})
        if self.case_type == 'OIEL':
            return factory.Faker(
                'random_element',
                elements=['CRYPTO', 'MEDIA', 'DEALER', 'MIL_DUAL', 'UKCONTSHELF'],
            ).generate({})

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
    status_control = factory.Faker('random_element', elements=['A', 'C', 'P'])
    licence_type = factory.Faker('random_element', elements=['OGEL', 'OIEL', 'OITCL'])
    el_id = factory.Faker('random_int', min=1, max=99999)
    usage_count = factory.Faker('random_int', min=0, max=1)

    @factory.lazy_attribute
    def elr_id(self):
        return set_pk(SPIREReturn, 'elr_id')

    @factory.lazy_attribute
    def ogl_id(self):
        if self.licence_type == 'OGEL':
            return factory.Faker('random_int', min=1, max=100).generate({})
        return

    class Meta:
        model = SPIREReturn


class SPIREThirdPartyFactory(BaseFactory):
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )
    batch = factory.SubFactory(SPIREBatchFactory)
    country_id = factory.Faker('random_int', min=1, max=200)
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=0, max=3)
    status_control = factory.Faker('random_element', elements=['A', 'C', 'P', 'D'])
    ultimate_end_user_flag = factory.Faker('random_element', elements=[0, 1, None])

    @factory.lazy_attribute
    def tp_id(self):
        return set_pk(SPIREThirdParty, 'tp_id')

    class Meta:
        model = SPIREThirdParty


class SPIREUltimateEndUserFactory(BaseFactory):
    application = factory.SubFactory(
        SPIREApplicationFactory, batch=factory.SelfAttribute('..batch')
    )
    batch = factory.SubFactory(SPIREBatchFactory)
    country_id = factory.Faker('random_int', min=1, max=200)
    status_control = factory.Faker('random_element', elements=['A', 'C', 'P', 'D'])
    start_date = factory.Faker('date_time_between', start_date='-2y', end_date='-1y')
    version_no = factory.Faker('random_int', min=0, max=3)

    @factory.lazy_attribute
    def ueu_id(self):
        return set_pk(SPIREUltimateEndUser, 'ueu_id')

    class Meta:
        model = SPIREUltimateEndUser


class PipelineFactory(BaseFactory):
    organisation = factory.Faker('word')
    dataset = factory.Faker('word')

    class Meta:
        model = Pipeline


class PipelineDataFileFactory(BaseFactory):
    pipeline = factory.SubFactory(PipelineFactory)
    data_file_url = factory.Faker('word')
    delimiter = factory.Faker('random_element', elements=[',', '\t', ';'])
    quote = factory.Faker('random_element', elements=['"', None])

    @factory.lazy_attribute
    def column_types(self):
        _types = []
        for i in range(0, random.randint(1, 10)):
            _types.append(
                [
                    factory.Faker('word').generate(),
                    random.choice(['text', 'int', 'boolean', 'decimal']),
                ]
            )
        return _types

    class Meta:
        model = PipelineDataFile
