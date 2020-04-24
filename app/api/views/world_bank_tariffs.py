from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import WorldBankTariffL0, WorldBankTariffTransformL1
from app.etl.organisation.world_bank import (
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline
)


class WorldBankTariffTransformListView(PipelinePaginatedListView):
    pipeline_column_types = WorldBankTariffTransformPipeline._l1_data_column_types
    model = WorldBankTariffTransformL1


class WorldBankTariffListView(PipelinePaginatedListView):
    pipeline_column_types = WorldBankTariffPipeline._l0_data_column_types
    model = WorldBankTariffL0
