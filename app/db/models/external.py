from app.db.models import (
    _col,
    _date,
    _int,
    _text,
    BaseModel,
)
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_reference_postcodes import ReferencePostcodesPipeline

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

    
class ReferencePostcodesL1(BaseModel):
    """
    Reference Postcode data

    A enriched view of the ONS postcode data with Data Engineering reference data
    """

    __tablename__ = 'L1'
    __table_args__ = {'schema': ReferencePostcodesPipeline.schema}

    id = _col(_int, primary_key=True, autoincrement=True)
    data_source_row_id = _col(_int, unique=True)
    post_code = _col(_text)
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
