from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_baci import DITBACIPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_world_bank_tariff import WorldBankBoundRatesPipeline, WorldBankTariffPipeline


def get_schemas():
    return [
        'operations',
        'admin',
        ONSPostcodeDirectoryPipeline.schema,
        DITReferencePostcodesPipeline.schema,
        WorldBankBoundRatesPipeline.schema,
        WorldBankTariffPipeline.schema,
        ComtradeCountryCodeAndISOPipeline.schema,
        DITEUCountryMembershipPipeline.schema,
        DITBACIPipeline.schema,
    ]
