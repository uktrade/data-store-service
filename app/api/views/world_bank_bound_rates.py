from app.api.views import ac, base, json_error
from app.db.models.external import WorldBankBoundRateL1
from app.etl.etl_world_bank_tariff import WorldBankBoundRatesPipeline


class WorldBankBoundRatesListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankBoundRatesPipeline
    model = WorldBankBoundRateL1
