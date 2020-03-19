import click
from common.db.models import HawkUsers
from flask.cli import with_appcontext


@click.command('add_hawk_user')
@with_appcontext
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
