import click
import sqlalchemy_utils
from flask import current_app as app
from flask.cli import with_appcontext


@click.command('db')
@with_appcontext
@click.option(
    '--create',
    is_flag=True,
    help='Create database using database name specified in (local) config',
)
@click.option(
    '--drop', is_flag=True, help='Drop database using database name specified in (local) config',
)
@click.option('--create_tables', is_flag=True, help='Create database tables')
@click.option(
    '--drop_tables', is_flag=True, help='Drop database tables',
)
@click.option(
    '--recreate_tables', is_flag=True, help='Drop and recreate database tables',
)
def db(create, drop, drop_tables, create_tables, recreate_tables):
    """
    Create/Drop database or database tables
    """
    if not any([create, drop, drop_tables, create_tables, recreate_tables]):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        db_url = app.config['SQLALCHEMY_DATABASE_URI']
        db_name = db_url.database
        if drop:
            click.echo(f'Dropping {db_name} database')
            sqlalchemy_utils.drop_database(db_url)
        if create:
            click.echo(f'Creating {db_name} database')
            sqlalchemy_utils.create_database(db_url, encoding='utf8')
        if drop_tables or recreate_tables:
            click.echo('Drop DB tables')
            app.db.drop_all()
        if create or create_tables or recreate_tables:
            click.echo('Creating DB tables')
            app.db.create_all()
