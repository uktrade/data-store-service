from flask import current_app as flask_app
from flask import request
from flask.views import View
from sqlalchemy.engine.reflection import Inspector


class TableDetailView(View):

    def dispatch_request(self, organisation, dataset):
        table_name = dataset
        schema = organisation

        table_exists = flask_app.db.engine.has_table(table_name, schema)
        if not table_exists:
            return flask_app.make_response(({}, 404),)

        response = {}
        response['columns'] = self.get_table(table_name, schema)
        return flask_app.make_response(response)

    def get_table(self, table_name, schema):
        insp = Inspector.from_engine(flask_app.db.engine)
        columns = insp.get_columns(table_name, schema=schema)
        return [{'name': col['name'], 'type': col['type']} for col in columns]
