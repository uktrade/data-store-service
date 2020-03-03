from app.api.views import ac, base, json_error
from app.db.models.external import WorldBankTariffL1
from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline


class WorldBankTariffListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankTariffPipeline
    model = WorldBankTariffL1
