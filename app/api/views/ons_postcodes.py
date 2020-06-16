from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import ONSPostcodeDirectoryL1
from app.etl.organisation.ons import ONSPostcodeDirectoryPipeline


class OnsPostcodeListView(PipelinePaginatedListView):
    pipeline_column_types = ONSPostcodeDirectoryPipeline._l1_data_column_types
    model = ONSPostcodeDirectoryL1
