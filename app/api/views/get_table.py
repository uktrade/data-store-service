from functools import wraps

from data_engineering.common.views import ac, base, json_error
from flask import current_app as flask_app
from flask.views import View
from sqlalchemy.engine.reflection import Inspector
from werkzeug.exceptions import NotFound, UnprocessableEntity


def _get_columns(schema, table_name):
    insp = Inspector.from_engine(flask_app.db.engine)
    columns = insp.get_columns(table_name, schema=schema)
    return [
        {'name': col['name'], 'type': col['type']}
        for col in columns
        if col['name'] not in ['data_hash', 'datafile_created', 'datafile_updated']
    ]


def table_valid(view_func):
    """Decorator that checks the table exists in the schema provided
    and that the schema is not in the excluded list of schemas.
    """

    @wraps(view_func)
    def handler(*args, **kwargs):
        schema = kwargs['schema']
        table_name = kwargs['table_name']

        table_exists = Inspector.from_engine(flask_app.db.engine).has_table(table_name, schema)
        if not table_exists or schema in [
            'pg_toast',
            'pg_temp_1',
            'pg_toast_temp_1',
            'pg_catalog',
            'public',
            'information_schema',
            'operations',
            'admin',
        ]:
            raise NotFound
        else:
            return view_func(*args, **kwargs)

    return handler


class TableStructureView(View):
    decorators = [table_valid, ac.authorization_required, ac.authentication_required, json_error]

    def dispatch_request(self, schema, table_name):
        response = {'columns': _get_columns(schema, table_name)}
        return flask_app.make_response(response)


class TableDataView(base.PaginatedListView):
    decorators = [table_valid, ac.authorization_required, ac.authentication_required, json_error]

    camel_case_columns = False
    include_id_column = True

    def dispatch_request(self, schema, table_name):
        self.schema = schema
        self.table_name = table_name
        return super().dispatch_request()

    def get_select_clause(self):
        columns = [column['name'] for column in _get_columns(self.schema, self.table_name)]
        # the id column is required for pagination
        if 'id' not in columns:
            raise UnprocessableEntity('The table must have an id column')
        return ','.join([f'"{c}"' for c in columns if c != 'id'])

    def get_from_clause(self):
        return f'"{self.schema}"."{self.table_name}"'
