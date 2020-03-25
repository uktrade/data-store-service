from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import WorldBankBoundRateL1
from app.etl.etl_world_bank_tariff import WorldBankBoundRatesPipeline


class WorldBankBoundRatesListView(PipelinePaginatedListView):
    pipeline_column_types = WorldBankBoundRatesPipeline._l1_data_column_types
    model = WorldBankBoundRateL1
