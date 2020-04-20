from app.db.models.external import SPIRERefCountryMapping
from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRERefCountryMappingPipeline(RebuildSchemaPipeline):

    dataset = 'country_mapping'
    organisation = 'spire'
    schema = SPIRERefCountryMapping.__table_args__['schema']
    sql_alchemy_model = SPIRERefCountryMapping
