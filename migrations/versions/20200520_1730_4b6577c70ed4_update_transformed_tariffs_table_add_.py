"""update transformed tariffs table & add indices to bounds rate

Revision ID: 4b6577c70ed4
Revises: 634088e167e9
Create Date: 2020-05-20 17:30:59.376612

"""
import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401


from app.db.models import get_schemas

revision = '4b6577c70ed4'
down_revision = '634088e167e9'


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
    op.create_index(
        op.f('ix_world_bank_bound_rates_L1_product'),
        'L1',
        ['product'],
        unique=False,
        schema=quoted_name('world_bank.bound_rates', quote=True),
    )
    op.create_index(
        op.f('ix_world_bank_bound_rates_L1_reporter'),
        'L1',
        ['reporter'],
        unique=False,
        schema=quoted_name('world_bank.bound_rates', quote=True),
    )
    op.add_column(
        'L1',
        sa.Column('eu_eu_rate', sa.Numeric(), nullable=True),
        schema=quoted_name('world_bank.tariff', quote=True),
    )
    op.alter_column(
        'L1',
        'partner',
        existing_type=sa.TEXT(),
        type_=sa.Integer(),
        existing_nullable=True,
        schema=quoted_name('world_bank.tariff', quote=True),
        postgresql_using='partner::integer',
    )
    op.alter_column(
        'L1',
        'reporter',
        existing_type=sa.TEXT(),
        type_=sa.Integer(),
        existing_nullable=True,
        schema=quoted_name('world_bank.tariff', quote=True),
        postgresql_using='reporter::integer',
    )
    op.drop_column('L1', 'country_average', schema=quoted_name('world_bank.tariff', quote=True))
    # ### end Alembic commands ###


def schema_downgrades():
    """schema downgrade migrations go here."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        'L1',
        sa.Column('country_average', sa.NUMERIC(), autoincrement=False, nullable=True),
        schema=quoted_name('world_bank.tariff', quote=True),
    )
    op.alter_column(
        'L1',
        'reporter',
        existing_type=sa.Integer(),
        type_=sa.TEXT(),
        existing_nullable=True,
        schema=quoted_name('world_bank.tariff', quote=True),
        postgresql_using='reporter::text',
    )
    op.alter_column(
        'L1',
        'partner',
        existing_type=sa.Integer(),
        type_=sa.TEXT(),
        existing_nullable=True,
        schema=quoted_name('world_bank.tariff', quote=True),
        postgresql_using='partner::text',
    )
    op.drop_column('L1', 'eu_eu_rate', schema=quoted_name('world_bank.tariff', quote=True))
    op.drop_index(
        op.f('ix_world_bank_bound_rates_L1_reporter'),
        table_name='L1',
        schema=quoted_name('world_bank.bound_rates', quote=True),
    )
    op.drop_index(
        op.f('ix_world_bank_bound_rates_L1_product'),
        table_name='L1',
        schema=quoted_name('world_bank.bound_rates', quote=True),
    )
    # ### end Alembic commands ###


def data_upgrades():
    """Add any optional data upgrade migrations here!"""
    pass


def data_downgrades():
    """Add any optional data downgrade migrations here!"""
    pass
