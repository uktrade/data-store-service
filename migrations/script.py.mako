"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision}
Create Date: ${create_date}

"""
import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401
${imports if imports else ""}

from app.db.models import get_schemas

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}


def create_schemas():
    conn = op.get_bind()
    for schema_name in get_schemas():
        if not conn.dialect.has_schema(conn, schema_name):
            conn.execute(sa.schema.CreateSchema(schema_name))



def upgrade():
    create_schemas()
    schema_upgrades()
    if context.get_x_argument(as_dictionary=True).get('data', None):
        data_upgrades()

def downgrade():
    if context.get_x_argument(as_dictionary=True).get('data', None):
        data_downgrades()
    schema_downgrades()

def schema_upgrades():
    """schema upgrade migrations go here."""
    ${upgrades if upgrades else "pass"}

def schema_downgrades():
    """schema downgrade migrations go here."""
    ${downgrades if downgrades else "pass"}

def data_upgrades():
    """Add any optional data upgrade migrations here!"""
    pass

def data_downgrades():
    """Add any optional data downgrade migrations here!"""
    pass