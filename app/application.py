import os
from logging.config import dictConfig

from flask import Flask, json
from sqlalchemy.engine.url import make_url

from app import config
from app.commands.dev import cmd_group as dev_cmd
from app.db.dbi import DBI

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {'level': 'INFO', 'handlers': ['console'], 'formatter': 'json'},
    'formatters': {
        'verbose': {'format': '[%(levelname)s] [%(name)s] %(message)s'},
        'json': {'()': 'app.api.settings.JSONLogFormatter'},
    },
    'handlers': {
        'console': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'json'}
    },
}

dictConfig(logging_config)


def get_or_create():
    from flask import current_app as flask_app

    if not flask_app:
        flask_app = _create_base_app()
    return flask_app


def make_current_app_test_app(test_db_name):
    flask_app = get_or_create()
    postgres_db_config = (
        os.environ.get('DATABASE_URL')
        if 'DATABASE_URL' in os.environ
        else config.get_config()['app']['database_url']
    )
    flask_app.config.update(
        {
            'TESTING': True,
            'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(
                postgres_db_config, test_db_name
            ),
        }
    )
    return flask_app


def _create_base_app():
    flask_app = Flask(__name__)
    flask_app.config.update(config.get_config())
    flask_app.cli.add_command(dev_cmd)

    postgres_db_config = (
        os.environ.get('DATABASE_URL')
        if 'DATABASE_URL' in os.environ
        else config.get_config()['app']['database_url']
    )
    flask_app.config.update(
        {
            'TESTING': False,
            'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(postgres_db_config),
            # set SQLALCHEMY_TRACK_MODIFICATIONS to False because
            # default of None produces warnings, and track modifications
            # are not required
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
        }
    )

    flask_app.secret_key = flask_app.config['app']['secret_key']
    from app.api.settings import CustomJSONEncoder

    flask_app.json_encoder = CustomJSONEncoder
    flask_app = _register_components(flask_app)
    return flask_app


def _register_components(flask_app):
    from app.api.views import api

    # Postgres DB
    from app.db import sql_alchemy

    sql_alchemy.session = sql_alchemy.create_scoped_session()
    sql_alchemy.init_app(flask_app)
    flask_app.db = sql_alchemy
    flask_app.dbi = DBI(sql_alchemy)

    # API
    flask_app.register_blueprint(api)

    return flask_app


def _load_uri_from_vcap_services(service_type):
    if 'VCAP_SERVICES' in os.environ:
        vcap_services = os.environ.get('VCAP_SERVICES')
        services = json.loads(vcap_services)
        if service_type in services:
            services_of_type = services[service_type]
            for service in services_of_type:
                if 'credentials' in service:
                    if 'uri' in service['credentials']:
                        return service['credentials']['uri']
    return None


def _create_sql_alchemy_connection_str(cfg, db_name=None):
    url = make_url(cfg)
    if db_name:
        url.database = db_name
    return url
