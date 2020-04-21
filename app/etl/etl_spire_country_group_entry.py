from app.db.models.external import SPIRECountryGroupEntry
from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRECountryGroupEntryPipeline(RebuildSchemaPipeline):

    dataset = 'country_group_entry'
    organisation = 'spire'
    schema = SPIRECountryGroupEntry.__table_args__['schema']
    sql_alchemy_model = SPIRECountryGroupEntry
