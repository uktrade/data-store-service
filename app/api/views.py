from functools import wraps

import redis
from flask import current_app as flask_app
from flask import jsonify, make_response, request
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized

from app.api.access_control import AccessControl
from app.api.utils import response_orientation_decorator, to_web_dict
from app.db.models.external import ONSPostcodeDirectoryL1
from app.db.models.internal import HawkUsers
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline

api = Blueprint(name="api", import_name=__name__)
ac = AccessControl()


@ac.client_key_loader
def get_client_key(client_id):
    client_key = HawkUsers.get_client_key(client_id)
    if client_key:
        return client_key
    else:
        raise LookupError()


@ac.client_scope_loader
def get_client_scope(client_id):
    client_scope = HawkUsers.get_client_scope(client_id)
    if client_scope:
        return client_scope
    else:
        raise LookupError()


@ac.nonce_checker
def seen_nonce(sender_id, nonce, timestamp):
    key = f'{sender_id}:{nonce}:{timestamp}'
    try:
        if flask_app.cache.get(key):
            # We have already processed this nonce + timestamp.
            return True
        else:
            # Save this nonce + timestamp for later.
            flask_app.cache.set(key, 'True', ex=300)
            return False
    except redis.exceptions.ConnectionError as e:
        flask_app.logger.error(f'failed to connect to caching server: {str(e)}')
        return True


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


# views
@api.route('/api/v1/get-ons-postcodes/', methods=['GET'])
@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_ons_postcodes(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_id = request.args.get('next-id')

    where = ''
    values = []

    if next_id is not None:
        where = 'where id >= %s'
        values = [next_id]

    sql_query = f'''
        select id, {','.join(
            [field for field, _ in ONSPostcodeDirectoryPipeline._l1_data_column_types]
        )}
        from {ONSPostcodeDirectoryL1.get_fq_table_name()}
        {where}
        order by id
        limit {pagination_size} + 1
    '''

    df = flask_app.dbi.execute_query(sql_query, data=values, df=True)
    if len(df) == pagination_size + 1:
        next_ = '{}{}?'.format(request.host_url[:-1], request.path)
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-id={}'.format(df['id'].values[-1])
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df.iloc[:, 1:], orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)
