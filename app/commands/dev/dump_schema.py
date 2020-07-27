import os
import subprocess

import click
from flask import current_app as app
from flask.cli import with_appcontext

from app.db.models.external import SPIRE_SCHEMA_NAME

FOLDER = f'{os.getcwd()}/.csv'

GET_TABLES_IN_SCHEMA_SQL = "select tablename from pg_tables where schemaname='{schema}'"


def get_table_names(schema):
    tables = app.dbi.execute_query(GET_TABLES_IN_SCHEMA_SQL.format(schema=schema))
    return [table for table, in tables]


def make_csv_folder(schema):
    folder = f'{FOLDER}/{schema}'
    os.makedirs(folder, exist_ok=True)
    return folder


def dump_table(db_uri, folder, schema, table):
    dump_sql = (
        f"""\\COPY \\"{schema}\\".\\"{table}\\" to '{folder}/{table.upper()}.csv' """
        f"WITH ("
        f"FORMAT "
        f"CSV, HEADER, "
        f"NULL 'NULL')"
    )  # noqa: W605
    cmd = f'psql {db_uri} -c "{dump_sql}"'
    click.echo(f'Creating {table.upper()}.csv')
    subprocess.run(cmd, shell=True)


@click.command('dump_schema')
@with_appcontext
@click.option(
    '--schema', type=click.Choice([SPIRE_SCHEMA_NAME]), help='Schema to dump to csv',
)
def dump_schema(schema):
    """
    Dump schema
    """
    if not schema:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        db_uri = app.config['SQLALCHEMY_DATABASE_URI']
        folder = make_csv_folder(schema)
        for table in get_table_names(schema):
            dump_table(db_uri, folder, schema, table)
