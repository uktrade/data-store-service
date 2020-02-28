import pandas as pd
from flask import current_app as flask_app
from flask.blueprints import Blueprint

from app.api.utils import to_web_dict
from app.api.views import ac, json_error

api = Blueprint(name="world_bank_tariffs_api", import_name=__name__)


@api.route('/api/v1/get-world-bank-tariffs/', methods=['GET'])
@json_error
@ac.authentication_required
@ac.authorization_required
def get_world_bank_tariffs():
    df = pd.DataFrame()
    next_ = None
    web_dict = to_web_dict(df.iloc[:, 1:])
    web_dict['next'] = next_

    return flask_app.make_response(web_dict)
