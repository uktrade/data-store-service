import app.db.models.external as models
from app.etl.rebuild_schema import RebuildSchemaPipeline


class SPIREPipeline(RebuildSchemaPipeline):
    organisation = 'spire'
    schema = models.SPIRE_SCHEMA_NAME

    
class SPIRECountryGroupPipeline(SPIREPipeline):

    dataset = models.SPIRECountryGroup.__tablename__
    sql_alchemy_model = models.SPIRECountryGroup


class SPIRECountryGroupEntryPipeline(SPIREPipeline):

    dataset = models.SPIRECountryGroupEntry.__tablename__
    sql_alchemy_model = models.SPIRECountryGroupEntry
