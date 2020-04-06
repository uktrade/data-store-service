from __future__ import with_statement

import logging
import re
from logging.config import fileConfig

from alembic import context
from alembic.script import write_hooks
from data_engineering.common.db.models import db
from flask import current_app
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.sql.schema import quoted_name

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')

config.set_main_option(
    'sqlalchemy.url', str(current_app.extensions['migrate'].db.engine.url).replace('%', '%%')
)

target_metadata = db.Model.metadata


@write_hooks.register("update_schemas")
def update_schema(filename, options):
    lines = []
    with open(filename) as file_:
        for line in file_:
            lines.append(
                re.sub(
                    r'schema=\'([A-Za-z._]*)\'', 'schema=quoted_name(\'\\g<1>\', quote=True)', line
                )
            )
    with open(filename, "w") as to_write:
        to_write.write("".join(lines))


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url, target_metadata=target_metadata, literal_binds=True, include_schemas=True
    )

    with context.begin_transaction():
        context.run_migrations()


def include_object(obj, name, type_, reflected, compare_to):
    if getattr(obj, 'schema', None):
        obj.schema = quoted_name(obj.schema, quote=True)
    return True


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # this callback is used to prevent an auto-migration from being generated
    # when there are no changes to the schema
    # reference: http://alembic.zzzcomputing.com/en/latest/cookbook.html
    def process_revision_directives(context, revision, directives):
        if getattr(config.cmd_opts, 'autogenerate', False):
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives[:] = []
                logger.info('No changes in schema detected.')

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=process_revision_directives,
            include_schemas=True,
            include_object=include_object,
            version_table_schema='public',
            **current_app.extensions['migrate'].configure_args
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
