"""Add SPIRE country and country group tables

Revision ID: 41b120f7846a
Revises: 49e6f321ee08
Create Date: 2020-04-16 11:26:07.568706

"""
import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401


from app.db.models import get_schemas

revision = '41b120f7846a'
down_revision = '49e6f321ee08'


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
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'country_group',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema=quoted_name('dit.spire', quote=True),
    )
    op.create_table(
        'ref_country_mapping',
        sa.Column('country_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('country_name', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('country_id'),
        schema=quoted_name('dit.spire', quote=True),
    )
    op.create_table(
        'country_group_entry',
        sa.Column('cg_id', sa.Integer(), nullable=True),
        sa.Column('country_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['cg_id'], ['dit.spire.country_group.id'],),
        sa.ForeignKeyConstraint(['country_id'], ['dit.spire.ref_country_mapping.country_id'],),
        schema=quoted_name('dit.spire', quote=True),
    )
    # ### end Alembic commands ###


def schema_downgrades():
    """schema downgrade migrations go here."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('country_group_entry', schema=quoted_name('dit.spire', quote=True))
    op.drop_table('ref_country_mapping', schema=quoted_name('dit.spire', quote=True))
    op.drop_table('country_group', schema=quoted_name('dit.spire', quote=True))
    # ### end Alembic commands ###


def data_upgrades():
    """Add any optional data upgrade migrations here!"""
    pass


def data_downgrades():
    """Add any optional data downgrade migrations here!"""
    pass
