import app.db.models.external as models
from app.etl.pipeline_type.rebuild_schema import RebuildSchemaPipeline


class SPIREPipeline(RebuildSchemaPipeline):
    organisation = 'spire'
    schema = models.SPIRE_SCHEMA_NAME


class SPIRECountryGroupPipeline(SPIREPipeline):

    dataset = models.SPIRECountryGroup.__tablename__
    sql_alchemy_model = models.SPIRECountryGroup


class SPIRECountryGroupEntryPipeline(SPIREPipeline):

    dataset = models.SPIRECountryGroupEntry.__tablename__
    sql_alchemy_model = models.SPIRECountryGroupEntry


class SPIRERefCountryMappingPipeline(SPIREPipeline):

    dataset = models.SPIRERefCountryMapping.__tablename__
    sql_alchemy_model = models.SPIRERefCountryMapping

