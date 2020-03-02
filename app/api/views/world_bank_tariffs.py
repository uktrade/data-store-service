from flask.blueprints import Blueprint

from app.api.views import ac, json_error
from app.api.views import base
from app.db.models.external import WorldBankTariffL1
from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline

api = Blueprint(name="world_bank_tariffs_api", import_name=__name__)


class WorldBankTariffListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = WorldBankTariffPipeline
    model = WorldBankTariffL1
