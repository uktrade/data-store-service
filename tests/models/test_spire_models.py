import datetime
import pytest
from sqlalchemy.exc import IntegrityError, DataError

from tests.fixtures.factories import (
    SPIREApplicationFactory,
    SPIREBatchFactory,
    SPIREFootnoteFactory,
    SPIREFootnoteEntryFactory,
    SPIREGoodsIncidentFactory,
    SPIREIncidentFactory,
    SPIREMediaFootnoteCountryFactory,
    SPIREMediaFootnoteDetailFactory,
    SPIREReturnFactory,
    SPIREThirdPartyFactory,
    SPIREUltimateEndUserFactory,
)


def test_application_fk_constraint(app_with_migrated_db):
    with pytest.raises(IntegrityError):
        SPIREApplicationFactory(batch=None)


@pytest.mark.parametrize(
    'case_type,raise_exception',
    (
        ('HELLO', True),
        ('OIEL', False),
    )
)
def test_application_check_constraint_1(app_with_migrated_db, case_type, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREApplicationFactory(case_type=case_type)
    else:
        SPIREApplicationFactory(case_type=case_type)


@pytest.mark.parametrize(
    'case_type,case_sub_type,raise_exception',
    (
        ('SIEL', 'PERMANENT', False),
        ('SIEL', 'TEMPORARY', False),
        ('SIEL', 'TRANSHIPMENT', False),
        ('SIEL', 'DEALER', True),
        ('OIEL', 'MEDIA', False),
        ('OIEL', 'MIL_DUAL', False),
        ('OIEL', 'UKCONTSHELF', False),
        ('OIEL', 'CRYPTO', False),
        ('OIEL', 'TEMPORARY', True),
        ('SITCL', None, False),
        ('OITCL', None, False),
        ('OGEL', None, False),
        ('GPL', None, False),
        ('TA_SIEL', None, False),
        ('TA_OIEL', None, False),
        ('GPL', 'MEDIA', True),
    )
)
def test_application_check_constraint_2(app_with_migrated_db, case_type, case_sub_type, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREApplicationFactory(case_type=case_type, case_sub_type=case_sub_type)
    else:
        SPIREApplicationFactory(case_type=case_type, case_sub_type=case_sub_type)


@pytest.mark.parametrize(
    'withheld_status,raise_exception',
    (
        ('HELLO', True),
        ('PENDING', False),
        ('WITHHELD', False),
    )
)
def test_application_check_constraint_3(app_with_migrated_db, withheld_status, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREApplicationFactory(withheld_status=withheld_status)
    else:
        SPIREApplicationFactory(withheld_status=withheld_status)


@pytest.mark.parametrize(
    'status,raise_exception',
    (
        ('HELLO', True),
        ('RELEASED', False),
        ('STAGING', False),
    )
)
def test_batch_check_constraint_1(app_with_migrated_db, status, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREBatchFactory(status=status)
    else:
        SPIREBatchFactory(status=status)


@pytest.mark.parametrize(
    'batch_ref,start_date,end_date,raise_exception',
    (
        (
            '10', None, None, None
        ),
        (
            '10', datetime.datetime(2020, 1, 1), datetime.datetime(2020, 2, 1), None,
        ),
        (
            'C10', None, None, True
        ),
        (
            '10', datetime.datetime(2020, 1, 1), None, True,
        )
    )
)
def test_batch_check_constraint_2(app_with_migrated_db, batch_ref, start_date, end_date, raise_exception):
    if not raise_exception:
        SPIREBatchFactory(batch_ref=batch_ref, start_date=start_date, end_date=end_date)
    else:
        with pytest.raises(IntegrityError):
            SPIREBatchFactory(batch_ref=batch_ref, start_date=start_date, end_date=end_date)


@pytest.mark.parametrize(
    'status,raise_exception',
    (
        ('HELLO', True),
        ('CURRENT', False),
        ('DELETED', False),
        ('ARCHIVED', False),
    )
)
def test_footnotes_check_constraint(app_with_migrated_db, status, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREFootnoteFactory(status=status)
    else:
        SPIREFootnoteFactory(status=status)


@pytest.mark.parametrize(
    'goods_item_id,country_id,fnr_id,raise_exception',
    (
        (1, None, None, False),
        (None, None, None, False),
        (None, 1, None, False),
        (None, 1, 1, False),
        (1, 1, 1, True),
        (1, None, 1, True),
        (1, 1, None, True),
    )
)
def test_footnote_entries_check_constraint_1(
    app_with_migrated_db, goods_item_id, country_id, fnr_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREFootnoteEntryFactory(goods_item_id=goods_item_id, country_id=country_id, fnr_id=fnr_id)
    else:
        SPIREFootnoteEntryFactory(goods_item_id=goods_item_id, country_id=country_id, fnr_id=fnr_id)


@pytest.mark.parametrize(
    'version_no,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_footnote_entries_check_constraint_2(
    app_with_migrated_db, version_no, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREFootnoteEntryFactory(version_no=version_no)
    else:
        SPIREFootnoteEntryFactory(version_no=version_no)


@pytest.mark.parametrize(
    'fn_id,mfd_id,mf_free_text,mf_grp_id,raise_exception',
    (
        (1, None, None, None, False),
        (None, 1, None, 1, False),
        (1, 1, 'HELLO', 1, False),
        (1, 1, None, 1, True),
        (None, None, None, 1, True),
        (None, 1, None, None, True),
        (None, None, None, None, True),
        (None, None, 'HELLO', None, True),
    ),
)
def test_footnote_entries_check_constraint_3(
    app_with_migrated_db, fn_id, mfd_id, mf_free_text, mf_grp_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREFootnoteEntryFactory(fn_id=fn_id, mf_grp_id=mf_grp_id, mfd_id=mfd_id,
                                      mf_free_text=mf_free_text)
    else:
        SPIREFootnoteEntryFactory(fn_id=fn_id, mf_grp_id=mf_grp_id, mfd_id=mfd_id,
                                  mf_free_text=mf_free_text)


@pytest.mark.parametrize(
    '_type,raise_exception',
    (
        ('HELLO', True),
        ('REFUSAL', False),
        ('WITHHELD', False),
        ('REVOKE', False),
        ('SURRENDER', False),
    )
)
def test_goods_incident_check_constraint_1(app_with_migrated_db, _type, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREGoodsIncidentFactory(type=_type)
    else:
        SPIREGoodsIncidentFactory(type=_type)


@pytest.mark.parametrize(
    'version_no,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_goods_incident_check_constraint_2(
    app_with_migrated_db, version_no, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREGoodsIncidentFactory(version_no=version_no)
    else:
        SPIREGoodsIncidentFactory(version_no=version_no)


@pytest.mark.parametrize(
    'status,raise_exception',
    (
        ('HELLO', True),
        ('RELEASED', True),
        ('STAGING', True),
        ('READY', False),
        ('FOR_ATTENTION', False),
    )
)
def test_incident_check_constraint_1(app_with_migrated_db, status, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(status=status)
    else:
        SPIREIncidentFactory(status=status)


@pytest.mark.parametrize(
    'version_no,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_2(
    app_with_migrated_db, version_no, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(version_no=version_no)
    else:
        SPIREIncidentFactory(version_no=version_no)


@pytest.mark.parametrize(
    'case_type,ogl_id,raise_exception',
    (
        ('GPL', None, False),
        ('OGEL', 1, False),
        ('OGEL', None, True),
        ('GPL', 1, True),
    ),
)
def test_incident_check_constraint_3(
    app_with_migrated_db, case_type, ogl_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(case_type=case_type, ogl_id=ogl_id)
    else:
        SPIREIncidentFactory(case_type=case_type, ogl_id=ogl_id)


@pytest.mark.parametrize(
    'temporary_licence_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_4(
    app_with_migrated_db, temporary_licence_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(temporary_licence_flag=temporary_licence_flag)
    else:
        SPIREIncidentFactory(temporary_licence_flag=temporary_licence_flag)


@pytest.mark.parametrize(
    '_type,licence_id,raise_exception',
    (
        ('REFUSAL', 1, False),
        ('REFUSAL', None, True),
        (None, None, True),
        ('ISSUE', 1, True),
    ),
)
def test_incident_check_constraint_5(
    app_with_migrated_db, _type, licence_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(type=_type, licence_id=licence_id)
    else:
        SPIREIncidentFactory(type=_type, licence_id=licence_id)


@pytest.mark.parametrize(
    '_type,else_id,raise_exception',
    (
        ('SUSPENSION', 1, False),
        ('SUSPENSION', None, True),
        (None, None, True),
        ('ISSUE', 1, True),
    ),
)
def test_incident_check_constraint_6(
    app_with_migrated_db, _type, else_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(type=_type, else_id=else_id)
    else:
        SPIREIncidentFactory(type=_type, else_id=else_id)


@pytest.mark.parametrize(
    '_type,raise_exception',
    (
        ('SUSPENSION', False),
        ('REFUSAL', False),
        ('ISSUE', False),
        ('REDUCTION', False),
        ('REVOKE', False),
        ('DEREGISTRATION', False),
        ('SURRENDER', False),
        ('HELLO', True),
    ),
)
def test_incident_check_constraint_7(
    app_with_migrated_db, _type, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(type=_type)
    else:
        SPIREIncidentFactory(type=_type)


@pytest.mark.parametrize(
    'case_type,raise_exception',
    (
        ('SIEL', False),
        ('OIEL', False),
        ('SITCL', False),
        ('OITCL', False),
        ('OGEL', False),
        ('GPL', False),
        ('TA_SIEL', False),
        ('TA_OIEL', False),
        ('HELLO', True),
    ),
)
def test_incident_check_constraint_8(
    app_with_migrated_db, case_type, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(case_type=case_type)
    else:
        SPIREIncidentFactory(case_type=case_type)


@pytest.mark.parametrize(
    'case_type,case_sub_type,raise_exception',
    (
        ('SIEL', 'PERMANENT', False),
        ('SIEL', 'TEMPORARY', False),
        ('SIEL', 'TRANSHIPMENT', False),
        ('SIEL', 'DEALER', True),
        ('OIEL', 'MEDIA', False),
        ('OIEL', 'MIL_DUAL', False),
        ('OIEL', 'UKCONTSHELF', False),
        ('OIEL', 'CRYPTO', False),
        ('OIEL', 'TEMPORARY', True),
        ('SITCL', None, False),
        ('OITCL', None, False),
        ('OGEL', None, False),
        ('GPL', None, False),
        ('TA_SIEL', None, False),
        ('TA_OIEL', None, False),
        ('GPL', 'MEDIA', True),
    )
)
def test_incident_check_constraint_9(app_with_migrated_db, case_type, case_sub_type, raise_exception):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREIncidentFactory(case_type=case_type, case_sub_type=case_sub_type)
    else:
        SPIREIncidentFactory(case_type=case_type, case_sub_type=case_sub_type)


@pytest.mark.parametrize(
    'licence_conversion_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_10(
    app_with_migrated_db, licence_conversion_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(licence_conversion_flag=licence_conversion_flag)
    else:
        SPIREIncidentFactory(licence_conversion_flag=licence_conversion_flag)


@pytest.mark.parametrize(
    'incorporation_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_11(
    app_with_migrated_db, incorporation_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(incorporation_flag=incorporation_flag)
    else:
        SPIREIncidentFactory(incorporation_flag=incorporation_flag)


@pytest.mark.parametrize(
    'mil_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_12(
    app_with_migrated_db, mil_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(mil_flag=mil_flag)
    else:
        SPIREIncidentFactory(mil_flag=mil_flag)


@pytest.mark.parametrize(
    'other_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_13(
    app_with_migrated_db, other_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(other_flag=other_flag)
    else:
        SPIREIncidentFactory(other_flag=other_flag)


@pytest.mark.parametrize(
    'torture_flag,raise_exception',
    (
        (1, False),
        (0, False),
        (-1, IntegrityError),
        (2, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_incident_check_constraint_14(
    app_with_migrated_db, torture_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREIncidentFactory(torture_flag=torture_flag)
    else:
        SPIREIncidentFactory(torture_flag=torture_flag)


@pytest.mark.parametrize(
    'status_control,end_datetime,raise_exception',
    (
        ('C', None, False),
        ('A', None, True),
        ('Z', None, True),
        (None, datetime.datetime(2020, 1, 1), False),
        ('C', datetime.datetime(2020, 1, 1), True),
    ),
)
def test_media_footnote_countries_check_1(
    app_with_migrated_db, status_control, end_datetime, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREMediaFootnoteCountryFactory(status_control=status_control, end_datetime=end_datetime)
    else:
        SPIREMediaFootnoteCountryFactory(status_control=status_control, end_datetime=end_datetime)


@pytest.mark.parametrize(
    'status_control,end_datetime,raise_exception',
    (
        ('C', None, False),
        ('A', None, True),
        ('Z', None, True),
        (None, datetime.datetime(2020, 1, 1), False),
        ('C', datetime.datetime(2020, 1, 1), True),
    ),
)
def test_media_footnote_detail_check_1(
    app_with_migrated_db, status_control, end_datetime, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREMediaFootnoteDetailFactory(status_control=status_control, end_datetime=end_datetime)
    else:
        SPIREMediaFootnoteDetailFactory(status_control=status_control, end_datetime=end_datetime)


@pytest.mark.parametrize(
    'footnote_type,raise_exception',
    (
        ('STANDARD', False),
        ('END_USER', False),
        ('HELLO', True),
    ),
)
def test_media_footnote_detail_check_2(
    app_with_migrated_db, footnote_type, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREMediaFootnoteDetailFactory(footnote_type=footnote_type)
    else:
        SPIREMediaFootnoteDetailFactory(footnote_type=footnote_type)


@pytest.mark.parametrize(
    'elr_version,raise_exception',
    (
        (10, None),
        (1, None),
        (0, IntegrityError),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_return_check_constraint_1(
    app_with_migrated_db, elr_version, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREReturnFactory(elr_version=elr_version)
    else:
        SPIREReturnFactory(elr_version=elr_version)


@pytest.mark.parametrize(
    'status,raise_exception',
    (
        ('HELLO', True),
        ('WITHDRAWN', False),
        ('ACTIVE', False),
    ),
)
def test_return_check_constraint_2(
    app_with_migrated_db, status, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREReturnFactory(status=status)
    else:
        SPIREReturnFactory(status=status)


@pytest.mark.parametrize(
    'status_control,raise_exception',
    (
        ('HELLO', True),
        ('A', False),
        ('P', False),
        ('C', False),
        ('D', True),
    ),
)
def test_return_check_constraint_3(
    app_with_migrated_db, status_control, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREReturnFactory(status_control=status_control)
    else:
        SPIREReturnFactory(status_control=status_control)


@pytest.mark.parametrize(
    'licence_type,ogl_id,raise_exception',
    (
        ('OGEL', 1, False),
        ('OGEL', None, True),
        ('OIEL', None, False),
        ('OITCL', None, False),
        ('OITCL', 1, True),
        ('HELLO', None, True),
    ),
)
def test_return_check_constraint_4_and_5(
    app_with_migrated_db, licence_type, ogl_id, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREReturnFactory(licence_type=licence_type, ogl_id=ogl_id)
    else:
        SPIREReturnFactory(licence_type=licence_type, ogl_id=ogl_id),


@pytest.mark.parametrize(
    'ultimate_end_user_flag,raise_exception',
    (
        (1, None),
        (0, None),
        (None, None),
        (-1, IntegrityError),
        (20, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_third_party_check_constraint_1(
    app_with_migrated_db, ultimate_end_user_flag, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREThirdPartyFactory(ultimate_end_user_flag=ultimate_end_user_flag)
    else:
        SPIREThirdPartyFactory(ultimate_end_user_flag=ultimate_end_user_flag)


@pytest.mark.parametrize(
    'version_no,raise_exception',
    (
        (56, False),
        (1, False),
        (0, False),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_third_party_check_constraint_2(
    app_with_migrated_db, version_no, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREThirdPartyFactory(version_no=version_no)
    else:
        SPIREThirdPartyFactory(version_no=version_no)


@pytest.mark.parametrize(
    'version_no,raise_exception',
    (
        (56, False),
        (1, False),
        (0, False),
        (-1, IntegrityError),
        ('HELLO', DataError)
    ),
)
def test_ultimate_end_user_check_constraint_1(
    app_with_migrated_db, version_no, raise_exception
):
    if raise_exception:
        with pytest.raises(raise_exception):
            SPIREUltimateEndUserFactory(version_no=version_no)
    else:
        SPIREUltimateEndUserFactory(version_no=version_no)


@pytest.mark.parametrize(
    'status_control,raise_exception',
    (
        ('HELLO', True),
        ('A', False),
        ('P', False),
        ('C', False),
        ('D', False),
        ('X', True),
    ),
)
def test_ultimate_end_user_check_constraint_2(
    app_with_migrated_db, status_control, raise_exception
):
    if raise_exception:
        with pytest.raises(IntegrityError):
            SPIREUltimateEndUserFactory(status_control=status_control)
    else:
        SPIREUltimateEndUserFactory(status_control=status_control)
