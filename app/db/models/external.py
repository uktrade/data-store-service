from app.db.models import (
    _col,
    _date,
    _float,
    _int,
    _text,
    BaseModel,
)
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline


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


class ProductMixin:

    selected_nomen = _col(_text)
    native_nomen = _col(_text)
    reporter = _col(_text)
    reporter_name = _col(_text)
    product = _col(_text)
    product_name = _col(_text)
    partner = _col(_text)
    partner_name = _col(_text)
    tariff_year = _col(_text)
    trade_year = _col(_text)
    trade_source = _col(_text)
    duty_type = _col(_text)
    simple_average = _col(_float)
    weighted_average = _col(_float)
    standard_deviation = _col(_float)
    minimum_rate = _col(_float)
    maximum_rate = _col(_float)
    nbr_of_total_lines = _col(_int)
    nbr_of_domestic_peaks = _col(_int)
    nbr_of_international_peaks = _col(_int)
    imports_value_in_1000_usd = _col(_float)
    binding_coverage = _col(_int)
    simple_tariff_line_average = _col(_float)
    variance = _col(_float)
    sum_of_rates = _col(_float)
    sum_of_s_avg_rates = _col(_float)
    count_of_s_avg_rates_cases = _col(_int)
    sum_of_squared_rates = _col(_float)
    nbr_of_ave_lines = _col(_int)
    nbr_of_na_lines = _col(_int)
    nbr_of_free_lines = _col(_int)
    nbr_of_dutiable_lines = _col(_int)
    nbr_line_0_to_5 = _col(_int)
    nbr_line_5_to_10 = _col(_int)
    nbr_line_10_to_20 = _col(_int)
    nbr_line_20_to_50 = _col(_int)
    nbr_line_50_to_100 = _col(_int)
    nbr_line_more_than_100 = _col(_int)
    sum_rate_by_wght_trd_value = _col(_float)
    sum_wght_trd_value_for_not_null = _col(_float)
    free_imports_in_1000_USD = _col(_float)
    dutiable_imports_in_1000_USD = _col(_float)
    specific_duty_imports_in_1000_USD = _col(_float)
    id = _col('id', _int, primary_key=True, autoincrement=True)


class WITSCleanedDataMixin:
    id = _col('id', _int, primary_key=True, autoincrement=True)
    product = _col(_text)
    reporter = _col(_text)
    partner = _col(_text)
    year = _col(_text)
    assumed_tariff = _col(_float)
    app_rate = _col(_text)
    mfn_rate = _col(_text)
    prf_rate = _col(_text)
    bnd_rate = _col(_text)
    country_average = _col(_text)
    world_average = _col(_float)


class Product040110(ProductMixin, BaseModel):
    __tablename__ = 'product_040110'
    __table_args__ = {'schema': 'public'}


class Product0201(ProductMixin, BaseModel):
    __tablename__ = 'product_0201'
    __table_args__ = {'schema': 'public'}


class WITSCleanedDataExample(WITSCleanedDataMixin, BaseModel):
    __tablename = 'wits_cleaned_data_example'
    __table_args__ = {'schema': 'public'}


class WITSCleanedData(WITSCleanedDataMixin, BaseModel):
    __tablename = 'wits_cleaned_data'
    __table_args__ = {'schema': 'public'}
