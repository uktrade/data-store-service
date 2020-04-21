"""Add check contraints

Revision ID: 8574fd28d877
Revises: e61160a7d500
Create Date: 2020-04-20 15:54:26.503932

"""
import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.sql.schema import quoted_name  # noqa: F401


from app.db.models import get_schemas

revision = '8574fd28d877'
down_revision = 'e61160a7d500'


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

    op.create_check_constraint(
        'applications_check_1',
        'applications',
        condition="""
            case_type IN ('SIEL', 'OIEL', 'SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'applications_check_2',
        'applications',
        condition="""
            (
                case_type = 'SIEL' AND case_sub_type IN ('PERMANENT', 'TEMPORARY', 'TRANSHIPMENT')
            )
            OR
            (
                case_type = 'OIEL'
                AND case_sub_type IN ('DEALER', 'MEDIA', 'MIL_DUAL', 'UKCONTSHELF','CRYPTO')
            )
            OR
            (
                case_type IN ('SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')
                AND case_sub_type IS NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'applications_check_3',
        'applications',
        condition="withheld_status IS NULL OR withheld_status IN ('PENDING', 'WITHHELD')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'batches_check_1',
        'batches',
        condition="status IN ('RELEASED','STAGING')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'batches_check_2',
        'batches',
        condition="""
            (
                batch_ref LIKE 'C%' AND start_date IS NULL AND end_date IS NULL
            )
            OR
            (
                batch_ref NOT LIKE 'C%'
                AND start_date IS NOT NULL
                AND date_trunc('day', start_date) = start_date
                AND end_date IS NOT NULL
                AND date_trunc('day', end_date) = end_date
                AND start_date <= end_date
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'footnotes_check',
        'footnotes',
        condition="status IN ('CURRENT', 'DELETED', 'ARCHIVED')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'footnote_entries_check_1',
        'footnote_entries',
        condition="""
            (
                goods_item_id IS NULL AND country_id IS NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NOT NULL AND country_id IS NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NULL AND country_id IS NOT NULL AND fnr_id IS NULL
            )
            OR
            (
                goods_item_id IS NULL AND country_id IS NOT NULL AND fnr_id IS NOT NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'footnote_entries_check_2',
        'footnote_entries',
        condition="version_no >= 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'footnote_entries_check_3',
        'footnote_entries',
        condition="""
            (
                fn_id IS NOT NULL AND mfd_id IS NULL AND mf_free_text IS NULL AND mf_grp_id IS NULL
            )
            OR
            (
                fn_id IS NULL
                AND mfd_id IS NOT NULL
                AND mf_free_text IS NULL
                AND mf_grp_id IS NOT NULL
            )
            OR
            (
                fn_id IS NULL
                AND mfd_id IS NULL
                AND mf_free_text IS NOT NULL
                AND mf_grp_id IS NOT NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'goods_incidents_check_1',
        'goods_incidents',
        condition="type IN ('REFUSAL', 'ISSUE', 'REVOKE',  'SURRENDER')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'goods_incidents_check_2',
        'goods_incidents',
        condition="version_no >= 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_1',
        'incidents',
        condition="status IN ('READY', 'FOR_ATTENTION')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_2',
        'incidents',
        condition="version_no >= 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_3',
        'incidents',
        condition="""
            (
                case_type != 'OGEL' AND ogl_id IS NULL
            )
            OR
            (
                case_type = 'OGEL' AND ogl_id IS NOT NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_4',
        'incidents',
        condition="temporary_licence_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_5',
        'incidents',
        condition="""
            (
                type != 'REFUSAL' AND licence_id IS NOT NULL
            )
            OR
            (
                type = 'REFUSAL' AND licence_id IS NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_6',
        'incidents',
        condition="""
            (
                type = 'SUSPENSION' AND else_id IS NOT NULL
            )
            OR
            (
                type != 'SUSPENSION' AND else_id IS NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_7',
        'incidents',
        condition="""
            type IN (
                'REFUSAL', 'ISSUE', 'REDUCTION', 'REVOKE', 'DEREGISTRATION',
                'SUSPENSION', 'SURRENDER'
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_8',
        'incidents',
        condition="""
            case_type IN (
                'SIEL', 'OIEL', 'SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL'
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_9',
        'incidents',
        condition="""
            (
                case_type = 'SIEL' AND case_sub_type IN ('PERMANENT', 'TEMPORARY', 'TRANSHIPMENT')
            )
            OR
            (
                case_type = 'OIEL'
                AND case_sub_type IN ('DEALER', 'MEDIA', 'MIL_DUAL', 'UKCONTSHELF','CRYPTO')
            )
            OR
            (
                case_type IN ('SITCL', 'OITCL', 'OGEL', 'GPL', 'TA_SIEL', 'TA_OIEL')
                AND case_sub_type IS NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_10',
        'incidents',
        condition="licence_conversion_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_11',
        'incidents',
        condition="incorporation_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_12',
        'incidents',
        condition="mil_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_13',
        'incidents',
        condition="other_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'incidents_check_14',
        'incidents',
        condition="torture_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'media_footnote_countries_check',
        'media_footnote_countries',
        condition="""
            (
                status_control = 'C' AND end_datetime IS NULL
            )
            OR
            (
                status_control IS NULL AND end_datetime IS NOT NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'media_footnote_details_check_1',
        'media_footnote_details',
        condition="""
            (
                status_control = 'C' AND end_datetime IS NULL
            )
            OR
            (
                status_control IS NULL AND end_datetime IS NOT NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'media_footnote_details_check_2',
        'media_footnote_details',
        condition="footnote_type IN ('STANDARD','END_USER')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'returns_check_1',
        'returns',
        condition="elr_version > 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'returns_check_2',
        'returns',
        condition="status IN ('WITHDRAWN', 'ACTIVE')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'returns_check_3',
        'returns',
        condition="status_control IN ('A','P','C')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'returns_check_4',
        'returns',
        condition="licence_type IN ('OGEL','OIEL','OITCL')",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'returns_check_5',
        'returns',
        condition="""
            (
                licence_type = 'OGEL' AND ogl_id IS NOT NULL
            )
            OR
            (
                licence_type != 'OGL' AND ogl_id IS NULL
            )
        """,
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'third_parties_check_1',
        'third_parties',
        condition="ultimate_end_user_flag IS NULL OR ultimate_end_user_flag IN (0, 1)",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'third_parties_check_2',
        'third_parties',
        condition="version_no >= 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'ultimate_end_users_check_1',
        'ultimate_end_users',
        condition="version_no >= 0",
        schema=quoted_name('spire', quote=True),
    )

    op.create_check_constraint(
        'ultimate_end_users_check_2',
        'ultimate_end_users',
        condition="status_control IN ('A', 'P', 'C', 'D')",
        schema=quoted_name('spire', quote=True),
    )


def schema_downgrades():
    """schema downgrade migrations go here."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        'applications_check_1',
        'applications',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'applications_check_2',
        'applications',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'applications_check_3',
        'applications',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'batches_check_1', 'batches', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'batches_check_2', 'batches', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'footnotes_check', 'footnotes', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'footnote_entries_check_1',
        'footnote_entries',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'footnote_entries_check_2',
        'footnote_entries',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'footnote_entries_check_3',
        'footnote_entries',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'goods_incidents_check_1',
        'goods_incidents',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'goods_incidents_check_2',
        'goods_incidents',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'incidents_check_1', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_2', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_3', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_4', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_5', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_6', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_7', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_8', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_9', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_10', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_11', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_12', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_13', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'incidents_check_14', 'incidents', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'media_footnote_countries_check',
        'media_footnote_countries',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'media_footnote_details_check_1',
        'media_footnote_details',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'media_footnote_details_check_2',
        'media_footnote_details',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'returns_check_1', 'returns', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'returns_check_2', 'returns', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'returns_check_3', 'returns', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'returns_check_4', 'returns', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'returns_check_5', 'returns', type_='check', schema=quoted_name('spire', quote=True)
    )
    op.drop_constraint(
        'third_parties_check_1',
        'third_parties',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'third_parties_check_2',
        'third_parties',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'ultimate_end_users_check_1',
        'ultimate_end_users',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )
    op.drop_constraint(
        'ultimate_end_users_check_2',
        'ultimate_end_users',
        type_='check',
        schema=quoted_name('spire', quote=True),
    )

    # ### end Alembic commands ###


def data_upgrades():
    """Add any optional data upgrade migrations here!"""
    pass


def data_downgrades():
    """Add any optional data downgrade migrations here!"""
    pass
