from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import DITBACIL1
from app.etl.etl_dit_baci import DITBACIPipeline


class BACIListView(PipelinePaginatedListView):
    pipeline_column_types = DITBACIPipeline._l1_data_column_types
    model = DITBACIL1
