from data_engineering.common.db.models import (
    _col,
    _date,
    _dt,
    _decimal,
    _foreign_key,
    _int,
    _relationship,
    _text,
    BaseModel,
)

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_baci import DITBACIPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_world_bank_tariff import (
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
    reporter = _col(_int)
    product = _col(_int)
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
    reporter = _col(_text)
    partner = _col(_text)
    year = _col(_int)
    assumed_tariff = _col(_decimal)
    app_rate = _col(_decimal)
    mfn_rate = _col(_decimal)
    bnd_rate = _col(_decimal)
    eu_rep_rate = _col(_decimal)
    eu_part_rate = _col(_decimal)
    country_average = _col(_decimal)
    world_average = _col(_decimal)


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
# SPIRE
#######################

SPIRE_SCHEMA_NAME = 'spire'


class SPIRECountryGroup(BaseModel):
    __tablename__ = f'country_group'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True, autoincrement=True)
    country_group_entries = _relationship('SPIRECountryGroupEntry', backref='country_group')


class SPIRERefCountryMapping(BaseModel):
    __tablename__ = 'ref_country_mapping'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    country_id = _col(_int, primary_key=True, autoincrement=True)
    country_name = _col(_text, nullable=False)


class SPIRECountryGroupEntry(BaseModel):
    __tablename__ = 'country_group_entry'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    cg_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.country_group.id'), primary_key=True)
    country_id = _col(_int, primary_key=True)


class SPIREBatch(BaseModel):
    __tablename__ = 'batch'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True, autoincrement=True)
    batch_ref = _col(_text, nullable=False)
    status = _col(_text, nullable=False)
    start_date = _col(_dt)
    end_date = _col(_dt)
    approve_date = _col(_dt, nullable=False)
    release_date = _col(_dt)
    staging_date = _col(_dt)

    application_countries = _relationship('SPIREApplicationCountry', backref='batch')
    application = _relationship('SPIREApplication', backref='batch')


class SPIREApplication(BaseModel):
    __tablename__ = 'application'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ela_grp_id = _col(_int, primary_key=True, autoincrement=True)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    initial_processing_time = _col(_int, nullable=False)
    case_closed_date = _col(_dt, nullable=False)
    withheld_status = _col(_text)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    ela_id = _col(_int)

    application_countries = _relationship('SPIREApplicationCountry', backref='application')
    application_amendments = _relationship('SPIREApplicationAmendment', backref='application')


class SPIREApplicationAmendment(BaseModel):
    __tablename__ = 'application_amendment'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), primary_key=True
    )
    ela_id = _col(_int, primary_key=True)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    case_processing_time = _col(_int, nullable=False)
    amendment_closed_date = _col(_dt, nullable=False)
    withheld_status = _col(_text)
    batch_id = _col(_int, nullable=False)


class SPIREApplicationCountry(BaseModel):
    __tablename__ = 'application_country'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ela_grp_id = _col(
        _int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), primary_key=True
    )
    country_id = _col(_int, primary_key=True)
    report_date = _col(_dt, nullable=False)
    start_date = _col(_dt, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)


class SPIREGoodsIncident(BaseModel):
    __tablename__ = 'goods_incident'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True, autoincrement=True)
    inc_id = _col(_int, nullable=False)
    type = _col(_text, nullable=False)
    goods_item_id = _col(_int, nullable=False)
    dest_country_id = _col(_int, nullable=False)
    source_country_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.country_group.id'))
    report_date = _col(_dt, nullable=False)
    ela_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), nullable=False)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    status_control = _col(_text, nullable=False)


class SPIREArs(BaseModel):
    __tablename__ = 'ars'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incident.id'), primary_key=True)
    ars_value = _col(_text, primary_key=True)
    ars_quantity = _col(_int)


class SPIRERefReportRating(BaseModel):
    __tablename__ = 'ref_report_ratings'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    rating = _col(_text, primary_key=True)
    report_rating = _col(_text, nullable=False)


class SPIREControlEntry(BaseModel):
    __tablename__ = 'control_entry'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incident.id'), primary_key=True)
    rating = _col(
        _text, _foreign_key(f'{SPIRE_SCHEMA_NAME}.ref_report_ratings.rating'), primary_key=True
    )
    value = _col(_int)


class SPIREEndUser(BaseModel):
    __tablename__ = 'end_user'
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
    __tablename__ = 'footnote'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    text = _col(_text)
    status = _col(_text, nullable=False)


class SPIREMediaFootnoteDetail(BaseModel):
    __tablename__ = 'media_footnote_detail'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    mf_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.media_footnote.id'), nullable=False)
    status_control = _col(_text)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)
    footnote_type = _col(_text, nullable=False)
    display_text = _col(_text, nullable=False)
    single_footnote_text = _col(_text, nullable=False)
    joint_footnote_text = _col(_text)


class SPIREFootnoteEntry(BaseModel):
    __tablename__ = 'footnote_entry'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    fne_id = _col(_int, primary_key=True)
    fn_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.footnote.id'))
    ela_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), nullable=False)
    goods_item_id = _col(_int)
    country_id = _col(_int)
    fnr_id = _col(_int)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    status_control = _col(_text, nullable=False)
    mfd_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.media_footnote_detail.id'))
    mf_grp_id = _col(_int)
    mf_free_text = _col(_text)


class SPIREIncident(BaseModel):
    __tablename__ = 'incident'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    inc_id = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    status = _col(_text, nullable=False)
    type = _col(_text, nullable=False)
    case_type = _col(_text, nullable=False)
    case_sub_type = _col(_text)
    ela_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), nullable=False)
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


class SPIREMediaFootnoteCountry(BaseModel):
    __tablename__ = 'media_footnote_country'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    ela_grp_id = _col(_int, nullable=False)
    mf_grp_id = _col(_int, nullable=False)
    country_id = _col(_int, nullable=False)
    country_name = _col(_text, nullable=False)
    status_control = _col(_text)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)


class SPIREMediaFootnote(BaseModel):
    __tablename__ = 'media_footnote'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)


class SPIREOglType(BaseModel):
    __tablename__ = 'ogl_type'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    id = _col(_int, primary_key=True)
    title = _col(_text, nullable=False)
    start_datetime = _col(_dt, nullable=False)
    end_datetime = _col(_dt)
    display_order = _col(_int)
    f680_flag = _col(_text)


class SPIREReasonForRefusal(BaseModel):
    __tablename__ = 'reason_for_refusal'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    gi_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.goods_incident.id'), primary_key=True)
    reason_for_refusal = _col(_text, primary_key=True)


class SPIRERefArsSubject(BaseModel):
    __tablename__ = 'ref_ars_subject'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ars_value = _col(_text, primary_key=True)
    ars_subject = _col(_text, nullable=False)


class SPIRERefDoNotReportValue(BaseModel):
    __tablename__ = 'ref_do_not_report_value'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    dnr_type = _col(_text, primary_key=True)
    dnr_value = _col(_text, primary_key=True)


class SPIREReturn(BaseModel):
    __tablename__ = 'return'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    elr_id = _col(_int, primary_key=True)
    elr_version = _col(_int, primary_key=True)
    status = _col(_text, nullable=False)
    created_datetime = _col(_dt, nullable=False)
    status_control = _col(_text, nullable=False)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    licence_type = _col(_text, nullable=False)
    el_id = _col(_int, nullable=False)
    ogl_id = _col(_int)
    return_period_date = _col(_dt)
    end_country_id = _col(_int)
    usage_count = _col(_int)
    end_user_type = _col(_text)
    eco_comment = _col(_text)


class SPIREThirdParty(BaseModel):
    __tablename__ = 'third_party'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    tp_id = _col(_int, primary_key=True)
    ela_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), nullable=False)
    sh_id = _col(_int)
    country_id = _col(_int, nullable=False)
    ultimate_end_user_flag = _col(_int)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    status_control = _col(_text, nullable=False)


class SPIREUltimateEndUser(BaseModel):
    __tablename__ = 'ultimate_end_user'
    __table_args__ = {'schema': SPIRE_SCHEMA_NAME}

    ueu_id = _col(_int, primary_key=True)
    ela_grp_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.application.ela_grp_id'), nullable=False)
    country_id = _col(_int, nullable=False)
    status_control = _col(_text, nullable=False)
    start_date = _col(_dt, nullable=False)
    version_no = _col(_int, primary_key=True)
    batch_id = _col(_int, _foreign_key(f'{SPIRE_SCHEMA_NAME}.batch.id'), nullable=False)
    sh_id = _col(_int)
    ultimate_end_user_flag = _col(_int)
