from common.views import ac, base, json_error

from app.db.models.external import ONSPostcodeDirectoryL1
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline


class OnsPostcodeListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = ONSPostcodeDirectoryPipeline
    model = ONSPostcodeDirectoryL1
