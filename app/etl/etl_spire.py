from app.db.models.external import (
    SPIRECountryGroup,
    SPIRECountryGroupEntry,
    SPIRERefCountryMapping
)
from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRECountryGroupPipeline(RebuildSchemaPipeline):

    dataset = 'country_group'
    organisation = 'spire'
    schema = SPIRECountryGroup.__table_args__['schema']
    sql_alchemy_model = SPIRECountryGroup


class SPIRECountryGroupEntryPipeline(RebuildSchemaPipeline):

    dataset = 'country_group_entry'
    organisation = 'spire'
    schema = SPIRECountryGroupEntry.__table_args__['schema']
    sql_alchemy_model = SPIRECountryGroupEntry


class SPIRERefCountryMappingPipeline(RebuildSchemaPipeline):

    dataset = 'country_mapping'
    organisation = 'spire'
    schema = SPIRERefCountryMapping.__table_args__['schema']
    sql_alchemy_model = SPIRERefCountryMapping
