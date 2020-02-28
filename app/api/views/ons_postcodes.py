from flask import current_app as flask_app
from flask import request
from flask.blueprints import Blueprint

from app.api.utils import response_orientation_decorator, to_web_dict
from app.api.views import ac, json_error
from app.db.models.external import ONSPostcodeDirectoryL1
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline

api = Blueprint(name="ons_postcodes_api", import_name=__name__)


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
