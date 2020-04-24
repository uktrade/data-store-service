from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import WorldBankBoundRateL0
from app.etl.organisation.world_bank import WorldBankBoundRatesPipeline


class WorldBankBoundRatesListView(PipelinePaginatedListView):
    pipeline_column_types = WorldBankBoundRatesPipeline._l0_data_column_types
    model = WorldBankBoundRateL0
