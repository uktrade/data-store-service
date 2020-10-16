import os

import sentry_sdk
from data_engineering.common.sso.register import register_sso_component
from flask_migrate import Migrate
from sentry_sdk.integrations.flask import FlaskIntegration

from app.commands import cmd_group

config_location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), 'config'))


def register_app_components(flask_app):
    if os.environ.get("SENTRY_DSN"):
        sentry_sdk.init(os.environ['SENTRY_DSN'], integrations=[FlaskIntegration()])

    Migrate(app=flask_app, db=flask_app.db, compare_type=True)

    from app.uploader.views import uploader_views

    flask_app.register_blueprint(uploader_views)

    register_sso_component(flask_app, role_based=False)

    if os.environ.get('NO_BROWSER_CACHE'):
        no_browser_cache(flask_app)

    flask_app.cli.add_command(cmd_group)

    return flask_app


def no_browser_cache(flask_app):
    @flask_app.after_request
    def add_header(response):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


static_location = f'{os.getcwd()}/static'
template_location = f'{os.getcwd()}/templates'
