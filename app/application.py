import os
from logging.config import dictConfig

from flask import Flask, json

from app import config


logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {'level': 'INFO', 'handlers': ['console']},
    'formatters': {'verbose': {'format': '[%(levelname)s] [%(name)s] %(message)s'}},
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        }
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
    return flask_app


def _create_base_app():
    flask_app = Flask(__name__)
    flask_app.config.update(config.get_config())

    flask_app.secret_key = flask_app.config['app']['secret_key']
    from app.api.settings import CustomJSONEncoder

    flask_app.json_encoder = CustomJSONEncoder
    flask_app = _register_components(flask_app)
    return flask_app


def _register_components(flask_app):
    from app.api.views import api

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
