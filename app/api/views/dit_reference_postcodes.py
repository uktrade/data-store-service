from flask import current_app as flask_app
from flask import request
from flask.views import View

from app.api.utils import to_web_dict
from app.api.views import ac, base, json_error
from app.db.models.external import DITReferencePostcodesL1
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline


class DitReferencePostcodeListView(base.PaginatedListView):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = DITReferencePostcodesPipeline
    model = DITReferencePostcodesL1


class DitReferencePostcodeView(View):

    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    pipeline = DITReferencePostcodesPipeline
    model = DITReferencePostcodesL1

    def dispatch_request(self):
        postcode = request.args.get('postcode')
        orientation = request.args.get('orientation', 'tabular')
        postcode = postcode.replace(' ', '').lower()
        sql_query = f'''
            select {','.join(
                [field for field, _ in DITReferencePostcodesPipeline._l1_data_column_types]
            )}
            from {DITReferencePostcodesL1.get_fq_table_name()}
            where lower(replace(postcode, ' ', '')) = %s
            limit 1
        '''
        df = flask_app.dbi.execute_query(sql_query, data=[postcode], df=True)
        web_dict = to_web_dict(df, orientation)
        return flask_app.make_response(web_dict)
