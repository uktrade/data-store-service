import os

from flask_migrate import Migrate

config_location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), 'config'))


def register_app_components(flask_app):
    Migrate(app=flask_app, db=flask_app.db, compare_type=True)

    @flask_app.teardown_appcontext
    def shutdown_session(exception=None):
        flask_app.db.session.remove()

    return flask_app
