from app.db.models.external import SPIRECountryGroup
from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRECountryGroupPipeline(RebuildSchemaPipeline):

    dataset = 'country_group'
    organisation = 'spire'
    schema = SPIRECountryGroup.__table_args__['schema']
    sql_alchemy_model = SPIRECountryGroup
