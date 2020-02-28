from flask import current_app as flask_app
from flask import request
from flask.blueprints import Blueprint
import pandas as pd

from app.api.utils import response_orientation_decorator, to_web_dict
from app.api.views import ac, json_error
from app.db.models.external import ONSPostcodeDirectoryL1
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline

api = Blueprint(name="world_bank_tariffs_api", import_name=__name__)


@api.route('/api/v1/get-world-bank-tariffs/', methods=['GET'])
@json_error
@ac.authentication_required
@ac.authorization_required
def get_world_bank_tariffs():
    pagination_size = flask_app.config['app']['pagination_size']
    next_id = request.args.get('next-id')

    where = ''
    values = []

    if next_id is not None:
        where = 'where id >= %s'
        values = [next_id]

    df = pd.DataFrame()
    next_ = None
    web_dict = to_web_dict(df.iloc[:, 1:])
    web_dict['next'] = next_

    return flask_app.make_response(web_dict)
