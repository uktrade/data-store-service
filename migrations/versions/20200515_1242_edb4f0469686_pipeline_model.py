"""Pipeline model

Revision ID: edb4f0469686
Revises: b1395ef0c894
Create Date: 2020-05-15 12:42:11.403787

"""
import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401


from app.db.models import get_schemas

revision = 'edb4f0469686'
down_revision = 'b1395ef0c894'


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
        'pipeline',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('organisation', sa.Text(), nullable=False),
        sa.Column('dataset', sa.Text(), nullable=False),
        sa.Column('slug', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organisation', 'dataset', name='organisation_dataset_unique_together'),
        schema=quoted_name('public', quote=True),
    )
    # ### end Alembic commands ###


def schema_downgrades():
    """schema downgrade migrations go here."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('pipeline', schema=quoted_name('public', quote=True))
    # ### end Alembic commands ###


def data_upgrades():
    """Add any optional data upgrade migrations here!"""
    pass


def data_downgrades():
    """Add any optional data downgrade migrations here!"""
    pass
