import os

from data_engineering.common.sso.register import register_sso_component
from flask_migrate import Migrate

config_location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), 'config'))


def register_app_components(flask_app):
    Migrate(app=flask_app, db=flask_app.db, compare_type=True)

    from app.uploader.views import uploader_views

    flask_app.register_blueprint(uploader_views)

    register_sso_component(flask_app, role_based=False)

    if os.environ.get('NO_BROWSER_CACHE'):
        no_browser_cache(flask_app)

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
