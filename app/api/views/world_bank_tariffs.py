from app.api.views import ac, base, json_error
from app.db.models.external import WorldBankTariffTransformL1
from app.etl.etl_world_bank_tariff import WorldBankTariffTransformPipeline


class WorldBankTariffTransformListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankTariffTransformPipeline
    model = WorldBankTariffTransformL1
