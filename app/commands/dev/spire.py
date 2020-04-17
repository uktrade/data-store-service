import os
import subprocess

import click
from flask import current_app as app
from flask.cli import with_appcontext

from app.db.models.external import SPIRE_SCHEMA_NAME


FOLDER = f'{os.getcwd()}/.csv/spire'

GET_TABLES_IN_SCHEMA_SQL = "select tablename from pg_tables where schemaname='{schema_name}'"


def get_table_names(schema_name):
    tables = app.dbi.execute_query(GET_TABLES_IN_SCHEMA_SQL.format(schema_name=schema_name))
    return [table for table, in tables]


def make_data_dir():
    os.makedirs(FOLDER, exist_ok=True)


@click.command('dump_spire_schema')
@with_appcontext
def dump_spire_schema():
    """
    Dump spire schema
    """
    name = SPIRE_SCHEMA_NAME
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    make_data_dir()
    for table in get_table_names(name):
        dump_sql = (
            f"""\COPY \\"{name}\\".\\"{table}\\" to '{FOLDER}/{table.upper()}.csv' """
            f"WITH ("
            f"FORMAT "
            f"CSV, HEADER)"
        )  # noqa: W605
        cmd = f'psql {db_uri} -c "{dump_sql}"'
        click.echo(f'Creating {table.upper()}.csv')
        subprocess.run(cmd, shell=True)


@click.command('populate_spire_schema')
@with_appcontext
@click.option(
    '--batch_size', type=int, help='Batch size', default=10,
)
def populate_spire_schema(batch_size):

    """
    Populate spire schema
    """
    click.echo('Populating schema')
    data_models = {
        **populate_country_mapping(batch_size),
        **populate_application(batch_size),
        **populate_ars(batch_size),
    }

    for data_type, _models in data_models.items():
        for _model in _models:
            app.db.session.add(_model)

    app.db.session.commit()


def populate_country_mapping(batch_size):
    click.echo('- Adding country data')
    from tests.fixtures.factories import (
        SPIRECountryGroupFactory,
        SPIRERefCountryMappingFactory,
        SPIRECountryGroupEntryFactory,
    )

    factories = {
        'country_group_entries': SPIRECountryGroupEntryFactory.create_batch(size=batch_size),
        'country_groups': SPIRECountryGroupFactory.create_batch(size=batch_size),
        'country_mappings': SPIRERefCountryMappingFactory.create_batch(size=batch_size),
    }
    return factories


def populate_application(batch_size):
    click.echo('- Adding application data')
    from tests.fixtures.factories import (
        SPIREBatchFactory,
        SPIREApplicationFactory,
        SPIREApplicationCountryFactory,
        SPIREApplicationAmendmentFactory,
    )

    factories = {
        'batches': SPIREBatchFactory.create_batch(size=batch_size),
        'application_countries': SPIREApplicationCountryFactory.create_batch(size=batch_size),
        'applications': SPIREApplicationFactory.create_batch(size=batch_size),
        'application_amendments': SPIREApplicationAmendmentFactory.create_batch(size=batch_size),
    }
    return factories


def populate_ars(batch_size):
    click.echo('- Adding ars data')
    from tests.fixtures.factories import (
        SPIREArsFactory,
        SPIREBatchFactory,
        SPIREGoodsIncidentFactory,
    )
    batch = SPIREBatchFactory()

    factories = {
        'goods_incident': SPIREGoodsIncidentFactory.create_batch(
            size=batch_size, batch=batch
        ),
        'ars': SPIREArsFactory.create_batch(size=batch_size),
    }
    return factories
