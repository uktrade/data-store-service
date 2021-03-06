from data_engineering.common.db.models import (
    _check,
    _col,
    _date,
    _decimal,
    _dt,
    _foreign_key,
    _int,
    _relationship,
    _text,
    BaseModel,
)
from sqlalchemy import Index

from app.etl.organisation.comtrade import ComtradeCountryCodeAndISOPipeline
from app.etl.organisation.dit import (
    DITBACIPipeline,
    DITEUCountryMembershipPipeline,
    DITReferencePostcodesPipeline,
)
from app.etl.organisation.ons import ONSPostcodeDirectoryPipeline
from app.etl.organisation.world_bank import (
    WorldBankBoundRatesPipeline,
    WorldBankTariffTransformPipeline,
)


class ONSPostcodeDirectoryL1(BaseModel):
    """
    ONS Postcode data

    All columns with E-codes are 9 in length
    Date formats are YYYYMM
    """

    __tablename__ = 'L1'
    __table_args__ = {'schema': ONSPostcodeDirectoryPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    pcd = _col(_text)  # Postcode XXX[space]YYY or XXXYYYY
    pcd2 = _col(_text)  # Postcode XXX[space][space]YYY or XXX[space]YYYY
    pcds = _col(_text, index=True)  # Postcode XXX[space]YYY[space] or XXX[space]YYYY
    dointr = _col(_date)  # Date of introduction YYYYMM
    doterm = _col(_date)  # Date of termination YYYYMM
    oscty = _col(_text)  # County E10
    ced = _col(_text)  # County Electoral Division E58
    oslaua = _col(_text)  # Local authority district (council) E06
    osward = _col(_text)  # Electoral ward/division E05
    parish = _col(_text)  # Parish/community E04
    usertype = _col(_text)  # Postcode user type (integer 0/1)
    oseast1m = _col(_text)  # National grid ref easting (numeric)
    osnrth1m = _col(_text)  # National grid ref northing (numeric)
    osgrdind = _col(_text)  # Grid reference (integer 1-6, 8-9)
    oshlthau = _col(_text)  # Health authority E18
    nhser = _col(_text)  # NHS England Region E40
    ctry = _col(_text)  # Country E92
    rgn = _col(_text)  # Region E12
    streg = _col(_text)  # Standard Statistical Region (integer 1-8)
    pcon = _col(_text)  # Westminster parliamentary constituency E14
    eer = _col(_text)  # European electoral region E15
    teclec = _col(_text)  # LLSC Local Learning Skills Council E24
    ttwa = _col(_text)  # Travel to work area E30
    pct = _col(_text)  # Primary care trust E16
    nuts = _col(_text)  # LAU2 Areas E05
    statsward = _col(_text)  # 2005 Statistical Ward 00AAFA
    oa01 = _col(_text)  # 2001 Census Output Area E00
    casward = _col(_text)  # Census Area Statistics Ward 00AAFA
    park = _col(_text)  # National Park E26
    lsoa01 = _col(_text)  # 2001 Census Lower Layer Super Output Area E01
    msoa01 = _col(_text)  # 2001 Census Middle Layer Super Output Area E02
    ur01ind = _col(_text)  # 2001 Census Urban/Rural Layer Super Output Area ([A-Z1-8])
    oac01 = _col(_text)  # 2001 Census Output Area classification 1A1
    oa11 = _col(_text)  # 2011 Census Output Area classification E00
    lsoa11 = _col(_text)  # 2011 Census Lower Layer Super Output Area E01
    msoa11 = _col(_text)  # 2011 Census Middle Layer Super Output Area E02
    wz11 = _col(_text)  # 2011 Census Workplace Zone E33
    ccg = _col(_text)  # Clinical Commissioning Group E38
    bua11 = _col(_text)  # Built-up Area E34
    buasd11 = _col(_text)  # Built-up Area Sub-division E35
    ru11ind = _col(_text)  # 2011 Census rural-urban classification A1
    oac11 = _col(_text)  # 2011 Census Output Area classification 1A1
    lat = _col(_text)  # Latitude (Numeric)
    long = _col(_text)  # Longitude (Numeric)
    lep1 = _col(_text)  # Local Enterprise Partnership(1) E37
    lep2 = _col(_text)  # Local Enterprise Partnership(2) E37
    pfa = _col(_text)  # Police force area E23
    imd = _col(_text)  # Index of Multiple Deprivation (Numeric)
    calncv = _col(_text)  # Cancer Alliance E56
    stp = _col(_text)  # Sustainability and Transformation Partnership E54
    publication_date = _col(_date)


class DITReferencePostcodesL1(BaseModel):
    """
    Reference Postcode data

    A enriched view of the ONS postcode data with Data Engineering reference data
    """

    __tablename__ = 'L1'
    __table_args__ = {'schema': DITReferencePostcodesPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    postcode = _col(_text)
    local_authority_district_code = _col(_text)
    local_authority_district_name = _col(_text)
    local_enterprise_partnership_lep1_code = _col(_text)
    local_enterprise_partnership_lep1_name = _col(_text)
    local_enterprise_partnership_lep2_code = _col(_text)
    local_enterprise_partnership_lep2_name = _col(_text)
    region_code = _col(_text)
    region_name = _col(_text)
    national_grid_ref_easting = _col(_text)
    national_grid_ref_northing = _col(_text)
    date_of_introduction = _col(_date)
    date_of_termination = _col(_date)


class WorldBankBoundRateL0(BaseModel):
    """
    Raw world bank bound rates
    """

    __tablename__ = 'L0'
    __table_args__ = {'schema': WorldBankBoundRatesPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    datafile_created = _col(_text)
    datafile_updated = _col(_text)
    data_hash = _col(_text, unique=True)
    nomen_code = _col(_text)
    reporter = _col(_int)
    product = _col(_int)
    bound_rate = _col(_decimal)
    total_number_of_lines = _col(_int)


class WorldBankBoundRateL1(BaseModel):
    """
    World bank bound rates
    """

    __tablename__ = 'L1'
    __table_args__ = {'schema': WorldBankBoundRatesPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    reporter = _col(_int, index=True)
    product = _col(_int, index=True)
    bound_rate = _col(_decimal)


class WorldBankTariffL0(BaseModel):
    """
    Raw world bank tariff data
    """

    __tablename__ = 'L0'
    __table_args__ = {'schema': WorldBankTariffTransformPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    datafile_created = _col(_text)
    reporter = _col(_int)
    year = _col(_int)
    product = _col(_int)
    partner = _col(_int)
    duty_type = _col(_text)
    simple_average = _col(_decimal)
    number_of_total_lines = _col(_int)


class WorldBankTariffTransformL1(BaseModel):
    """
    World bank tariff data
    """

    __tablename__ = 'L1'
    __table_args__ = {'schema': WorldBankTariffTransformPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    product = _col(_int)
    reporter = _col(_int)
    partner = _col(_int)
    year = _col(_int)
    assumed_tariff = _col(_decimal)
    app_rate = _col(_decimal)
    mfn_rate = _col(_decimal)
    bnd_rate = _col(_decimal)
    eu_rep_rate = _col(_decimal)
    eu_part_rate = _col(_decimal)
    eu_eu_rate = _col(_decimal)
    world_average = _col(_decimal)

    Index(f"{__tablename__}_product_idx", product, postgresql_using='hash')
    Index(f"{__tablename__}_reporter_idx", reporter, postgresql_using='hash')
    Index(f"{__tablename__}_partner_idx", partner, postgresql_using='hash')
    Index(f"{__tablename__}_year_idx", year, postgresql_using='hash')


class ComtradeCountryCodeAndISOL1(BaseModel):

    __tablename__ = 'L1'
    __table_args__ = {'schema': ComtradeCountryCodeAndISOPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    cty_code = _col(_int)
    cty_name_english = _col(_text)
    cty_fullname_english = _col(_text)
    cty_abbreviation = _col(_text)
    cty_comments = _col(_text)
    iso2_digit_alpha = _col(_text)
    iso3_digit_alpha = _col(_text)
    start_valid_year = _col(_text)
    end_valid_year = _col(_text)


class DITEUCountryMembershipL1(BaseModel):

    __tablename__ = 'L1'
    __table_args__ = {'schema': DITEUCountryMembershipPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    country = _col(_text)
    iso3 = _col(_text)
    year = _col(_int)
    tariff_code = _col(_text)


class DITBACIL1(BaseModel):

    __tablename__ = 'L1'
    __table_args__ = {'schema': DITBACIPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    year = _col(_int)
    product_category = _col(_int)
    exporter = _col(_int)
    importer = _col(_int)
    trade_flow_value = _col(_decimal)
    quantity = _col(_decimal)


#######################
# SPIRE models

# Check constraints are not picked up by alembic when generating migrations automatically.
# They therefore have to be generated manually to match the models.

#######################

SPIRE_SCHEMA_NAME = 'spire'


class SPIRECountryGroup(BaseModel):
    __tablename__ = 'country_groups'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True, autoincrement=True)

    country_group_entries = _relationship('SPIRECountryGroupEntry', backref='country_group')
    goods_incidents = _relationship('SPIREGoodsIncident', backref='country_group')


class SPIRERefCountryMapping(BaseModel):
    __tablename__ = 'ref_country_mappings'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    country_id = _col(_int, primary_key=True, autoincrement=True)
    country_name = _col(_text, nullable=False)


class SPIRECountryGroupEntry(BaseModel):
    __tablename__ = 'country_group_entries'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    cg_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.country_groups.id'), primary_key=True)
    country_id = _col(_int, primary_key=True)


class SPIREBatch(BaseModel):
    __tablename__ = 'batches'

    id = _col(_int, primary_key=True, autoincrement=True)
    batch_ref = _col(_text, nullable=False)
    status = _col(_text, nullable=False)
    start_date = _col(_dt)
    end_date = _col(_dt)
    approve_date = _col(_dt, nullable=False)
    release_date = _col(_dt)
    staging_date = _col(_dt)

    application_countries = _relationship('SPIREApplicationCountry', backref='batch')
    applications = _relationship('SPIREApplication', backref='batch')
    goods_incidents = _relationship('SPIREGoodsIncident', backref='batch')
    footnote_entries = _relationship('SPIREFootnoteEntry', backref='batch')
    incidents = _relationship('SPIREIncident', backref='batch')
    returns = _relationship('SPIREReturn', backref='batch')
    third_parties = _relationship('SPIREThirdParty', backref='batch')
    ultimate_end_users = _relationship('SPIREUltimateEndUser', backref='batch')

    __table_args__ = (
        _check("status IN ('RELEASED','STAGING')"),
        _check(
            """
            (
                batch_ref LIKE 'C%' AND start_date IS NULL AND end_date IS NULL
            )
            OR
            (
                batch_ref NOT LIKE 'C%'
                AND start_date IS NOT NULL
                AND date_trunc('day', start_date) = start_date
                AND end_date IS NOT NULL
                AND date_trunc('day', end_date) = end_date
                AND start_date <= end_date
            )
        """
        ),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREApplication(BaseModel):
    __tablename__ = 'applications'

    ela_grp_id = _col(_int, primary_key=True, autoincrement=True)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    initial_processing_time = _col(_int, nullable=False)
    case_closed_date = _col(_dt, nullable=False)
    withheld_status = _col(_text)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    ela_id = _col(_int)

    application_countries = _relationship('SPIREApplicationCountry', backref='application')
    application_amendments = _relationship('SPIREApplicationAmendment', backref='application')
    goods_incidents = _relationship('SPIREGoodsIncident', backref='application')
    footnote_entries = _relationship('SPIREFootnoteEntry', backref='application')
    incidents = _relationship('SPIREIncident', backref='application')
    third_parties = _relationship('SPIREThirdParty', backref='application')
    ultimate_end_users = _relationship('SPIREUltimateEndUser', backref='application')

    __table_args__ = (
        _check(
            "case_type IN ('SIEL', 'OIEL', 'SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')"
        ),
        _check(
            """
            (
                case_type = 'SIEL' AND case_sub_type IN ('PERMANENT', 'TEMPORARY', 'TRANSHIPMENT')
            )
            OR
            (
                case_type = 'OIEL'
                AND case_sub_type IN ('DEALER', 'MEDIA', 'MIL_DUAL', 'UKCONTSHELF','CRYPTO')
            )
            OR
            (
                case_type IN ('SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')
                AND case_sub_type IS NULL
            )
        """
        ),
        _check("withheld_status IS NULL OR withheld_status IN ('PENDING', 'WITHHELD')"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREApplicationAmendment(BaseModel):
    __tablename__ = 'application_amendments'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), primary_key=True
    )
    ela_id = _col(_int, primary_key=True)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    case_processing_time = _col(_int, nullable=False)
    amendment_closed_date = _col(_dt, nullable=False)
    withheld_status = _col(_text)
    batch_id = _col(_int, nullable=False)


class SPIREApplicationCountry(BaseModel):
    __tablename__ = 'application_countries'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), primary_key=True
    )
    country_id = _col(_int, primary_key=True)
    report_date = _col(_dt, nullable=False)
    start_date = _col(_dt, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)


class SPIREGoodsIncident(BaseModel):
    __tablename__ = 'goods_incidents'

    id = _col(_int, primary_key=True, autoincrement=True)
    inc_id = _col(_int, nullable=False)
    type = _col(_text, nullable=False)
    goods_item_id = _col(_int, nullable=False)
    dest_country_id = _col(_int, nullable=False)
    source_country_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.country_groups.id'))
    report_date = _col(_dt, nullable=False)
    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), nullable=False
    )
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    status_control = _col(_text, nullable=False)

    ars = _relationship('SPIREArs', backref="goods_incident")
    reasons_for_refusal = _relationship('SPIREReasonForRefusal', backref='goods_incident')
    control_entries = _relationship('SPIREControlEntry', backref='goods_incident')

    __table_args__ = (
        _check("type IN ('REFUSAL', 'ISSUE', 'REVOKE',  'SURRENDER')"),
        _check("version_no >= 0"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREArs(BaseModel):
    __tablename__ = 'ars'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incidents.id'), primary_key=True)
    ars_value = _col(_text, primary_key=True)
    ars_quantity = _col(_int)


class SPIRERefReportRating(BaseModel):
    __tablename__ = 'ref_report_ratings'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    rating = _col(_text, primary_key=True)
    report_rating = _col(_text, nullable=False)

    control_entries = _relationship('SPIREControlEntry', backref='ref_report_rating')


class SPIREControlEntry(BaseModel):
    __tablename__ = 'control_entries'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incidents.id'), primary_key=True)
    rating = _col(
        _text, _foreign_key(f'{SPIRE_SCHEMA_NAME}.ref_report_ratings.rating'), primary_key=True
    )
    value = _col(_decimal)


class SPIREEndUser(BaseModel):
    __tablename__ = 'end_users'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    eu_id = _col(_int, primary_key=True)
    version_number = _col(_int, primary_key=True)
    ela_grp_id = _col(_int)
    end_user_type = _col(_text)
    country_id = _col(_int)
    end_user_count = _col(_int)
    start_date = _col(_dt)
    status_control = _col(_text)
    batch_id = _col(_int)


class SPIREFootnote(BaseModel):
    __tablename__ = 'footnotes'

    id = _col(_int, primary_key=True)
    text = _col(_text)
    status = _col(_text, nullable=False)
    footnote_entries = _relationship('SPIREFootnoteEntry', backref='footnote')

    __table_args__ = (
        _check("status IN ('CURRENT', 'DELETED', 'ARCHIVED')"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREMediaFootnoteDetail(BaseModel):
    __tablename__ = 'media_footnote_details'

    id = _col(_int, primary_key=True)
    mf_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.media_footnotes.id'), nullable=False)
    status_control = _col(_text)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)
    footnote_type = _col(_text, nullable=False)
    display_text = _col(_text, nullable=False)
    single_footnote_text = _col(_text, nullable=False)
    joint_footnote_text = _col(_text)

    footnote_entries = _relationship('SPIREFootnoteEntry', backref='media_footnote_detail')

    __table_args__ = (
        _check(
            """
            (
                status_control = 'C' AND end_datetime IS NULL
            )
            OR
            (
                status_control IS NULL AND end_datetime IS NOT NULL
            )
        """
        ),
        _check("footnote_type IN ('STANDARD','END_USER')"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREFootnoteEntry(BaseModel):
    __tablename__ = 'footnote_entries'

    fne_id = _col(_int, primary_key=True)
    fn_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.footnotes.id'))
    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), nullable=False
    )
    goods_item_id = _col(_int)
    country_id = _col(_int)
    fnr_id = _col(_int)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    status_control = _col(_text, nullable=False)
    mfd_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.media_footnote_details.id'))
    mf_grp_id = _col(_int)
    mf_free_text = _col(_text)

    __table_args__ = (
        _check(
            """
            (
                goods_item_id IS NULL AND country_id IS NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NOT NULL AND country_id IS NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NULL AND country_id IS NOT NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NULL AND country_id IS NOT NULL AND fnr_id IS NOT NULL
            )
        """
        ),
        _check("version_no >= 0"),
        _check(
            """
            (
                fn_id IS NOT NULL AND mfd_id IS NULL AND mf_free_text IS NULL AND mf_grp_id IS NULL
            )
            OR
            (
                fn_id IS NULL
                AND mfd_id IS NOT NULL
                AND mf_free_text IS NULL
                AND mf_grp_id IS NOT NULL
            )
            OR
            (
                fn_id IS NULL
                AND mfd_id IS NULL
                AND mf_free_text IS NOT NULL
                AND mf_grp_id IS NOT NULL
            )
        """
        ),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREIncident(BaseModel):
    __tablename__ = 'incidents'

    inc_id = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    status = _col(_text, nullable=False)
    type = _col(_text, nullable=False)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), nullable=False
    )
    ela_id = _col(_int)
    licence_id = _col(_int)
    report_date = _col(_dt, nullable=False)
    temporary_licence_flag = _col(_int, nullable=False)
    licence_conversion_flag = _col(_int, nullable=False)
    incorporation_flag = _col(_int, nullable=False)
    mil_flag = _col(_int, nullable=False)
    other_flag = _col(_int, nullable=False)
    torture_flag = _col(_int, nullable=False)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True)
    ogl_id = _col(_int)
    status_control = _col(_text, nullable=False)
    else_id = _col(_int)
    stakeholders_confirmed = _col(_text)

    __table_args__ = (
        _check("status IN ('READY', 'FOR_ATTENTION')"),
        _check("version_no >= 0"),
        _check(
            """
            (
                case_type != 'OGEL' AND ogl_id IS NULL
            )
            OR
            (
                case_type = 'OGEL' AND ogl_id IS NOT NULL
            )
        """
        ),
        _check("temporary_licence_flag IN (0, 1)"),
        _check(
            """
            (
                type != 'REFUSAL' AND licence_id IS NOT NULL
            )
            OR
            (
                type = 'REFUSAL' AND licence_id IS NULL
            )
        """
        ),
        _check(
            """
            (
                type = 'SUSPENSION' AND else_id IS NOT NULL
            )
            OR
            (
                type != 'SUSPENSION' AND else_id IS NULL
            )
        """
        ),
        _check(
            """
            type IN (
                'REFUSAL', 'ISSUE', 'REDUCTION', 'REVOKE', 'DEREGISTRATION',
                'SUSPENSION', 'SURRENDER'
            )
        """
        ),
        _check(
            """
            case_type IN (
                'SIEL', 'OIEL', 'SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL'
            )
        """
        ),
        _check(
            """
            (
                case_type = 'SIEL' AND case_sub_type IN ('PERMANENT', 'TEMPORARY', 'TRANSHIPMENT')
            )
            OR
            (
                case_type = 'OIEL'
                AND case_sub_type IN ('DEALER', 'MEDIA', 'MIL_DUAL', 'UKCONTSHELF','CRYPTO')
            )
            OR
            (
                case_type IN ('SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')
                AND case_sub_type IS NULL
            )
        """
        ),
        _check("licence_conversion_flag IN (0, 1)"),
        _check("incorporation_flag IN (0, 1)"),
        _check("mil_flag IN (0, 1)"),
        _check("other_flag IN (0, 1)"),
        _check("torture_flag IN (0, 1)"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREMediaFootnoteCountry(BaseModel):
    __tablename__ = 'media_footnote_countries'

    id = _col(_int, primary_key=True)
    ela_grp_id = _col(_int, nullable=False)
    mf_grp_id = _col(_int, nullable=False)
    country_id = _col(_int, nullable=False)
    country_name = _col(_text, nullable=False)
    status_control = _col(_text)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)

    __table_args__ = (
        _check(
            """
            (
                status_control = 'C' AND end_datetime IS NULL
            )
            OR
            (
                status_control IS NULL AND end_datetime IS NOT NULL
            )
        """
        ),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREMediaFootnote(BaseModel):
    __tablename__ = 'media_footnotes'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    media_footnote_details = _relationship('SPIREMediaFootnoteDetail', backref='media_footnote')


class SPIREOglType(BaseModel):
    __tablename__ = 'ogl_types'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    title = _col(_text, nullable=False)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)
    display_order = _col(_int)
    f680_flag = _col(_text)


class SPIREReasonForRefusal(BaseModel):
    __tablename__ = 'reasons_for_refusal'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incidents.id'), primary_key=True)
    reason_for_refusal = _col(_text, primary_key=True)


class SPIRERefArsSubject(BaseModel):
    __tablename__ = 'ref_ars_subjects'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ars_value = _col(_text, primary_key=True)
    ars_subject = _col(_text, nullable=False)


class SPIRERefDoNotReportValue(BaseModel):
    __tablename__ = 'ref_do_not_report_values'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    dnr_type = _col(_text, primary_key=True)
    dnr_value = _col(_text, primary_key=True)


class SPIREReturn(BaseModel):
    __tablename__ = 'returns'

    elr_id = _col(_int, primary_key=True)
    elr_version = _col(_int, primary_key=True)
    status = _col(_text, nullable=False)
    created_datetime = _col(_dt, nullable=False)
    status_control = _col(_text, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    licence_type = _col(_text, nullable=False)
    el_id = _col(_int, nullable=False)
    ogl_id = _col(_int)
    return_period_date = _col(_dt)
    end_country_id = _col(_int)
    usage_count = _col(_int)
    end_user_type = _col(_text)
    eco_comment = _col(_text)

    __table_args__ = (
        _check("elr_version > 0"),
        _check("status IN ('WITHDRAWN', 'ACTIVE')"),
        _check("status_control IN ('A','P','C')"),
        _check("licence_type IN ('OGEL','OIEL','OITCL')"),
        _check(
            """
            (
                licence_type = 'OGEL' AND ogl_id IS NOT NULL
            )
            OR
            (
                licence_type != 'OGEL' AND ogl_id IS NULL
            )
        """
        ),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREThirdParty(BaseModel):
    __tablename__ = 'third_parties'

    tp_id = _col(_int, primary_key=True)
    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), nullable=False
    )
    sh_id = _col(_int)
    country_id = _col(_int, nullable=False)
    ultimate_end_user_flag = _col(_int)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    status_control = _col(_text, nullable=False)

    __table_args__ = (
        _check("ultimate_end_user_flag IS NULL OR ultimate_end_user_flag IN (0, 1)"),
        _check("version_no >= 0"),
        {'schema': SPIRE_SCHEMA_NAME},
    )


class SPIREUltimateEndUser(BaseModel):
    __tablename__ = 'ultimate_end_users'

    ueu_id = _col(_int, primary_key=True)
    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.applications.ela_grp_id'), nullable=False
    )
    country_id = _col(_int, nullable=False)
    status_control = _col(_text, nullable=False)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batches.id'), nullable=False)
    sh_id = _col(_int)
    ultimate_end_user_flag = _col(_int)

    __table_args__ = (
        _check("version_no >= 0"),
        _check("status_control IN ('A', 'P', 'C', 'D')"),
        {'schema': SPIRE_SCHEMA_NAME},
    )
