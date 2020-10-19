"""add column to pipeline_data_file to track when processing began

Revision ID: ac7dbd72d46e
Revises: 2258ff389578
Create Date: 2020-10-19 22:37:52.754276

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401

revision = 'ac7dbd72d46e'
down_revision = '2258ff389578'


def upgrade():
    schema_upgrades()


def downgrade():
    schema_downgrades()


def schema_upgrades():
    op.add_column(
        'pipeline_data_file', sa.Column('started_processing_at', sa.DateTime(), nullable=True)
    )


def schema_downgrades():
    op.drop_column('pipeline_data_file', 'started_processing_at')
