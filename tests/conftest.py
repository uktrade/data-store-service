import pytest
import sqlalchemy_utils
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig

pytest_plugins = [
    "tests.fixtures.add_to_db",
    "data_engineering.common.tests.conftest",
]


@pytest.fixture(scope='module')
def app_with_migrated_db(app):
    app.db.session.close_all()
    app.db.engine.dispose()
    sqlalchemy_utils.create_database(app.config['SQLALCHEMY_DATABASE_URI'])
    alembic_config = AlembicConfig('migrations/alembic.ini')
    alembic_config.set_main_option('sqlalchemy.url', str(app.config['SQLALCHEMY_DATABASE_URI']))
    alembic_config.set_main_option('script_location', 'migrations')
    alembic_upgrade(alembic_config, 'head')
    yield app
    app.db.session.close()
    app.db.session.remove()
    sqlalchemy_utils.drop_database(app.config['SQLALCHEMY_DATABASE_URI'])
