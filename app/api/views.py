from functools import wraps

from flask import current_app as flask_app
from flask import jsonify, make_response
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized


api = Blueprint(name="api", import_name=__name__)


def json_error(f):
    @wraps(f)
    def error_handler(*args, **kwargs):
        try:
            response = f(*args, **kwargs)
        except NotFound:
            response = jsonify({})
            response.status_code = 404
        except BadRequest as e:
            response = jsonify({'error': e.description})
            response.status_code = 400
        except Unauthorized:
            response = make_response('')
            response.status_code = 401
        except Exception as e:
            flask_app.logger.error(f'unexpected exception for API request: {str(e)}')
            response = make_response('')
            response.status_code = 500
        return response

    return error_handler


@api.route('/healthcheck/', methods=["GET"])
def healthcheck():
    return jsonify({"status": "OK"})
