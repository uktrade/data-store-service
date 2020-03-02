from flask.blueprints import Blueprint

from app.api.views import ac, base, json_error
from app.db.models.external import ONSPostcodeDirectoryL1
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline

api = Blueprint(name="ons_postcodes_api", import_name=__name__)


class OnsPostcodeListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = ONSPostcodeDirectoryPipeline
    model = ONSPostcodeDirectoryL1
