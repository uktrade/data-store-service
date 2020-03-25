from data_engineering.common.views import ac, base, json_error

from app.db.models.external import WorldBankTariffL0, WorldBankTariffTransformL1
from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline, WorldBankTariffTransformPipeline


class WorldBankTariffTransformListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankTariffTransformPipeline
    model = WorldBankTariffTransformL1


class WorldBankTariffListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankTariffPipeline
    model = WorldBankTariffL0

    def get_fields(self):
        return self.get_field_types_from_column_types(self.pipeline._l0_data_column_types)
