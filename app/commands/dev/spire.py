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
    populate_country_mapping(batch_size)
    app.db.session.commit()


def populate_country_mapping(batch_size):
    click.echo('- Adding country data')
    from tests.fixtures.factories import SPIRECountryGroupFactory, SPIRERefCountryMappingFactory

    batch = []

    batch_group = SPIRECountryGroupFactory.create_batch(size=batch_size)

    country_mappings = SPIRERefCountryMappingFactory.create_batch(
        size=batch_size
    )
    batch.extend(country_mappings)
    batch.extend(batch_group)

    for _country_model in batch:
        app.db.session.add(_country_model)
