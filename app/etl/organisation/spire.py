import app.db.models.external as models
from app.etl.pipeline_type.rebuild_schema import RebuildSchemaPipeline


class SPIREPipeline(RebuildSchemaPipeline):
    organisation = 'dit'
    dataset = 'spire'
    schema = models.SPIRE_SCHEMA_NAME


class SPIRECountryGroupPipeline(SPIREPipeline):
    subsubdataset = models.SPIRECountryGroup.__tablename__
    sql_alchemy_model = models.SPIRECountryGroup


class SPIRECountryGroupEntryPipeline(SPIREPipeline):
    subdataset = models.SPIRECountryGroupEntry.__tablename__
    sql_alchemy_model = models.SPIRECountryGroupEntry


class SPIRERefCountryMappingPipeline(SPIREPipeline):
    subdataset = models.SPIRERefCountryMapping.__tablename__
    sql_alchemy_model = models.SPIRERefCountryMapping


class SPIREBatchPipeline(SPIREPipeline):
    subdataset = models.SPIREBatch.__tablename__
    sql_alchemy_model = models.SPIREBatch


class SPIREApplicationPipeline(SPIREPipeline):
    subdataset = models.SPIREApplication.__tablename__
    sql_alchemy_model = models.SPIREApplication


class SPIREApplicationAmendmentPipeline(SPIREPipeline):
    subdataset = models.SPIREApplicationAmendment.__tablename__
    sql_alchemy_model = models.SPIREApplicationAmendment


class SPIREApplicationCountryPipeline(SPIREPipeline):
    subdataset = models.SPIREApplicationCountry.__tablename__
    sql_alchemy_model = models.SPIREApplicationCountry


class SPIREGoodsIncidentPipeline(SPIREPipeline):
    subdataset = models.SPIREGoodsIncident.__tablename__
    sql_alchemy_model = models.SPIREGoodsIncident


class SPIREArsPipeline(SPIREPipeline):
    subdataset = models.SPIREArs.__tablename__
    sql_alchemy_model = models.SPIREArs


class SPIRERefReportRatingPipeline(SPIREPipeline):
    subdataset = models.SPIRERefReportRating.__tablename__
    sql_alchemy_model = models.SPIRERefReportRating


class SPIREControlEntryPipeline(SPIREPipeline):
    subdataset = models.SPIREControlEntry.__tablename__
    sql_alchemy_model = models.SPIREControlEntry


class SPIREEndUserPipeline(SPIREPipeline):
    subdataset = models.SPIREEndUser.__tablename__
    sql_alchemy_model = models.SPIREEndUser


class SPIREFootnotePipeline(SPIREPipeline):
    subdataset = models.SPIREFootnote.__tablename__
    sql_alchemy_model = models.SPIREFootnote


class SPIREMediaFootnoteDetailPipeline(SPIREPipeline):
    subdataset = models.SPIREMediaFootnoteDetail.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnoteDetail


class SPIREFootnoteEntryPipeline(SPIREPipeline):
    subdataset = models.SPIREFootnoteEntry.__tablename__
    sql_alchemy_model = models.SPIREFootnoteEntry


class SPIREIncidentPipeline(SPIREPipeline):
    subdataset = models.SPIREIncident.__tablename__
    sql_alchemy_model = models.SPIREIncident


class SPIREMediaFootnoteCountryPipeline(SPIREPipeline):
    subdataset = models.SPIREMediaFootnoteCountry.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnoteCountry


class SPIREMediaFootnotePipeline(SPIREPipeline):
    subdataset = models.SPIREMediaFootnote.__tablename__
    sql_alchemy_model = models.SPIREMediaFootnote


class SPIREOglTypePipeline(SPIREPipeline):
    subdataset = models.SPIREOglType.__tablename__
    sql_alchemy_model = models.SPIREOglType


class SPIREReasonForRefusalPipeline(SPIREPipeline):
    subdataset = models.SPIREReasonForRefusal.__tablename__
    sql_alchemy_model = models.SPIREReasonForRefusal


class SPIRERefArsSubjectPipeline(SPIREPipeline):
    subdataset = models.SPIRERefArsSubject.__tablename__
    sql_alchemy_model = models.SPIRERefArsSubject


class SPIRERefDoNotReportValuePipeline(SPIREPipeline):
    subdataset = models.SPIRERefDoNotReportValue.__tablename__
    sql_alchemy_model = models.SPIRERefDoNotReportValue


class SPIREReturnPipeline(SPIREPipeline):
    subdataset = models.SPIREReturn.__tablename__
    sql_alchemy_model = models.SPIREReturn


class SPIREThirdPartyPipeline(SPIREPipeline):
    subdataset = models.SPIREThirdParty.__tablename__
    sql_alchemy_model = models.SPIREThirdParty


class SPIREUltimateEndUserPipeline(SPIREPipeline):
    subdataset = models.SPIREUltimateEndUser.__tablename__
    sql_alchemy_model = models.SPIREUltimateEndUser
