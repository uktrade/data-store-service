from app.db.models.external import SPIRE_SCHEMA_NAME
from app.etl.organisation.comtrade import ComtradeCountryCodeAndISOPipeline
from app.etl.organisation.dit import (
    DITBACIPipeline,
    DITEUCountryMembershipPipeline,
    DITReferencePostcodesPipeline,
)
from app.etl.organisation.ons import ONSPostcodeDirectoryPipeline
from app.etl.organisation.world_bank import WorldBankBoundRatesPipeline, WorldBankTariffPipeline


def get_schemas():
    return [
        'public',
        'operations',
        'admin',
        ONSPostcodeDirectoryPipeline.schema,
        DITReferencePostcodesPipeline.schema,
        WorldBankBoundRatesPipeline.schema,
        WorldBankTariffPipeline.schema,
        ComtradeCountryCodeAndISOPipeline.schema,
        DITEUCountryMembershipPipeline.schema,
        DITBACIPipeline.schema,
        SPIRE_SCHEMA_NAME,
    ]
