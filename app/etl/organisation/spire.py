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


class SPIRERefCountryMappingPipeline(SPIREPipeline):

    dataset = models.SPIRERefCountryMapping.__tablename__
    sql_alchemy_model = models.SPIRERefCountryMapping

    
class SPIREBatchPipeline(SPIREPipeline):

    dataset = models.SPIREBatch.__tablename__
    sql_alchemy_model = models.SPIREBatch

    
class SPIREApplicationPipeline(SPIREPipeline):

    dataset = models.SPIREApplication.__tablename__
    sql_alchemy_model = models.SPIREApplication

    
class SPIREApplicationAmendmentPipeline(SPIREPipeline):

    dataset = models.SPIREApplicationAmendment.__tablename__
    sql_alchemy_model = models.SPIREApplicationAmendment

    
class SPIREApplicationCountryPipeline(SPIREPipeline):

    dataset = models.SPIREApplicationCountry.__tablename__
    sql_alchemy_model = models.SPIREApplicationCountry

    
class SPIREGoodsIncidentPipeline(SPIREPipeline):

    dataset = models.SPIREGoodsIncident.__tablename__
    sql_alchemy_model = models.SPIREGoodsIncident

    
class SPIREArsPipeline(SPIREPipeline):

    dataset = models.SPIREArs.__tablename__
    sql_alchemy_model = models.SPIREArs

    
class SPIRERefReportRatingPipeline(SPIREPipeline):

    dataset = models.SPIRERefReportRating.__tablename__
    sql_alchemy_model = models.SPIRERefReportRating

    
class SPIREControlEntryPipeline(SPIREPipeline):

    dataset = models.SPIREControlEntry.__tablename__
    sql_alchemy_model = models.SPIREControlEntry

    
class SPIREEndUserPipeline(SPIREPipeline):

    dataset = models.SPIREEndUser.__tablename__
    sql_alchemy_model = models.SPIREEndUser

    
class SPIREFootnotePipeline(SPIREPipeline):

    dataset = models.SPIREFootnote.__tablename__
    sql_alchemy_model = models.SPIREFootnote

    
class SPIREMediaFootnoteDetailPipeline(SPIREPipeline):

    dataset = models.SPIREMediaFootnoteDetail.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnoteDetail

    
class SPIREFootnoteEntryPipeline(SPIREPipeline):

    dataset = models.SPIREFootnoteEntry.__tablename__
    sql_alchemy_model = models.SPIREFootnoteEntry

    
class SPIREIncidentPipeline(SPIREPipeline):

    dataset = models.SPIREIncident.__tablename__
    sql_alchemy_model = models.SPIREIncident

    
class SPIREMediaFootnoteCountryPipeline(SPIREPipeline):

    dataset = models.SPIREMediaFootnoteCountry.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnoteCountry

    
class SPIREMediaFootnotePipeline(SPIREPipeline):

    dataset = models.SPIREMediaFootnote.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnote

    
class SPIREOglTypePipeline(SPIREPipeline):

    dataset = models.SPIREOglType.__tablename__
    sql_alchemy_model = models.SPIREOglType

    
class SPIREReasonForRefusalPipeline(SPIREPipeline):

    dataset = models.SPIREReasonForRefusal.__tablename__
    sql_alchemy_model = models.SPIREReasonForRefusal

    
class SPIRERefArsSubjectPipeline(SPIREPipeline):

    dataset = models.SPIRERefArsSubject.__tablename__
    sql_alchemy_model = models.SPIRERefArsSubject

    
class SPIRERefDoNotReportValuePipeline(SPIREPipeline):

    dataset = models.SPIRERefDoNotReportValue.__tablename__
    sql_alchemy_model = models.SPIRERefDoNotReportValue

    
class SPIREReturnPipeline(SPIREPipeline):

    dataset = models.SPIREReturn.__tablename__
    sql_alchemy_model = models.SPIREReturn

    
class SPIREThirdPartyPipeline(SPIREPipeline):

    dataset = models.SPIREThirdParty.__tablename__
    sql_alchemy_model = models.SPIREThirdParty

    
class SPIREUltimateEndUserPipeline(SPIREPipeline):

    dataset = models.SPIREUltimateEndUser.__tablename__
    sql_alchemy_model = models.SPIREUltimateEndUser
