import click
import sqlalchemy_utils
from flask import current_app as app
from flask.cli import AppGroup, with_appcontext

from app.db.models.internal import HawkUsers
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.manager import Manager as PipelineManager

cmd_group = AppGroup('dev', help='Commands to build database')


@cmd_group.command('db')
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


@cmd_group.command('add_hawk_user')
@click.option('--client_id', type=str, help="a unique id for the client")
@click.option(
    '--client_key', type=str, help="secret key only known by the client and server",
)
@click.option(
    '--client_scope', type=str, help="comma separated list of endpoints",
)
@click.option(
    '--description', type=str, help="describe the usage of these credentials",
)
def add_hawk_user(client_id, client_key, client_scope, description):
    """
    Add hawk user
    """
    if not all([client_id, client_key, client_scope, description]):
        click.echo('All parameters are required')
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        client_scope_list = client_scope.split(',')
        HawkUsers.add_user(
            client_id=client_id,
            client_key=client_key,
            client_scope=client_scope_list,
            description=description,
        )


arg_to_pipeline_config_list = {
    # format:  {'command option': [(pipeline, dataset subdir)]}
    # pipeline processing is grouped by data_source,
    # except when multiple format versions are supported or
    # pipelines are dependent on other pipelines
    ONSPostcodeDirectoryPipeline.data_source: [
        (ONSPostcodeDirectoryPipeline, 'ons/postcode_directory/')
    ],
}


@cmd_group.command('datafiles_to_events_by_source')
@click.option('--all', is_flag=True, help='all datafiles to events')
def datafiles_to_events_by_source(**kwargs):
    manager = PipelineManager(storage=app.config['inputs']['source-folder'], dbi=app.dbi)
    for arg, pipeline_info_list in arg_to_pipeline_config_list.items():
        arg = arg.replace(".", "__")
        if kwargs['all'] or kwargs[arg]:
            for pipeline, sub_dir in pipeline_info_list:
                manager.pipeline_register(pipeline=pipeline, sub_directory=sub_dir)
    manager.pipeline_process_all()


def _pipeline_option(option_name):
    return click.option(
        f'--{option_name}',
        f'{option_name.replace(".", "__")}',
        is_flag=True,
        help=f'{option_name} data to events',
    )


for k in arg_to_pipeline_config_list.keys():
    datafiles_to_events_by_source = _pipeline_option(k)(datafiles_to_events_by_source)
