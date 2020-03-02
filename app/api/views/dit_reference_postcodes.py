from flask import current_app as flask_app
from flask import request
from flask.blueprints import Blueprint
from flask.views import View

from app.api.utils import to_web_dict
from app.api.views import ac, json_error
from app.api.views import base
from app.db.models.external import DITReferencePostcodesL1
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline

api = Blueprint(name="dir_reference_postcodes_api", import_name=__name__)


class DitReferencePostcodeListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = DITReferencePostcodesPipeline
    model = DITReferencePostcodesL1


class DitReferencePostcodeView(base.SingleResourceView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = DITReferencePostcodesPipeline
    model = DITReferencePostcodesL1
