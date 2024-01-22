from data_engineering.common.api.utils import to_web_dict
from data_engineering.common.views import ac, json_error
from flask import current_app as flask_app
from flask import request
from flask.views import View
from werkzeug.exceptions import BadRequest

from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import DITReferencePostcodesL1
from app.etl.organisation.dit import DITReferencePostcodesPipeline


class DitReferencePostcodeListView(PipelinePaginatedListView):
    pipeline_column_types = DITReferencePostcodesPipeline._l1_data_column_types
    model = DITReferencePostcodesL1


class DitReferencePostcodeView(View):
    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = DITReferencePostcodesPipeline
    model = DITReferencePostcodesL1

    def dispatch_request(self):
        orientation = request.args.get('orientation', 'tabular')
        postcode = request.args.get('postcode')
        if not postcode:
            raise BadRequest('No postcode specified')
        postcode = postcode.replace(' ', '').lower()
        sql_query = f'''
            select {','.join(
                [field for field, _ in DITReferencePostcodesPipeline._l1_data_column_types]
            )}
            from {DITReferencePostcodesL1.get_fq_table_name()}
            where lower(replace(postcode, ' ', '')) = :postcode
            limit 1
        '''
        df = flask_app.dbi.execute_query(sql_query, data={'postcode': postcode}, df=True)
        web_dict = to_web_dict(df, orientation)
        return flask_app.make_response(web_dict)
