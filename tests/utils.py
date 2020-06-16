import datetime
import re
from decimal import Decimal
from uuid import UUID

from data_engineering.common.tests.utils import row_to_sortable_tuple

timezone_regex = re.compile(r'.*\d{2}:\d{2}(\+|-)\d{2}:\d{2}$')


def rows_equal_table(dbi, expected_rows, table, pipeline, order_matters=False, top_rows=None):
    db_rows = _table_to_rows(dbi, table, pipeline)

    if not order_matters:
        expected_rows.sort(key=row_to_sortable_tuple)
        db_rows.sort(key=row_to_sortable_tuple)

    if top_rows:
        db_rows = db_rows[:top_rows]

    if len(expected_rows) != len(db_rows):
        print(f'Number of rows are not equal: {len(expected_rows)} expected, db has {len(db_rows)}')
        return False

    result = expected_rows == db_rows
    if not result:
        print(f'Rows do not match:\nEXPECTED: {list(expected_rows)}\nACT   DB: {list(db_rows)}')
    return result


def _table_to_rows(dbi, table, pipeline):
    query = f"""
       SELECT * FROM {table}
    """
    rows = dbi.execute_query(query)
    # strip columns that are not required for validation
    if 'L0' in table:
        rows = [r[len(pipeline.l0_helper_columns) :] for r in rows]
    elif 'L1' in table:
        rows = [r[len(pipeline.l1_helper_columns) :] for r in rows]

    def _format(value):
        if isinstance(value, datetime.datetime):
            string_value = str(value)
            if timezone_regex.match(string_value):
                # Removing TZ info in string (running tests in UTC timezone so no need to transform)
                r = string_value[:-6]
            else:
                r = string_value
        elif isinstance(value, datetime.date) or isinstance(value, UUID):
            r = str(value)
        elif isinstance(value, Decimal):
            r = float(value)
        else:
            r = value
        return r

    return [tuple(_format(value) for value in r) for r in rows]
