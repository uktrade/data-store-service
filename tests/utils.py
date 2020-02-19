import datetime
import re
from decimal import Decimal
from uuid import UUID

import pandas as pd

timezone_regex = re.compile(r'.*\d{2}:\d{2}(\+|-)\d{2}:\d{2}$')


def rows_equal_query_results(dbi, expected_rows, query, order_matters=False):
    db_rows = _query_result_to_rows(dbi, query)

    if len(expected_rows) != len(db_rows):
        print(
            'Number of rows are not equal:' f'{len(expected_rows)} expected, db has {len(db_rows)}'
        )
        return False

    if not order_matters:
        expected_rows.sort(key=_row_to_sortable_tuple)
        db_rows.sort(key=_row_to_sortable_tuple)

    return expected_rows == db_rows


def rows_equal_table(dbi, expected_rows, table, order_matters=False, top_rows=None):
    db_rows = _table_to_rows(dbi, table)

    if not order_matters:
        expected_rows.sort(key=_row_to_sortable_tuple)
        db_rows.sort(key=_row_to_sortable_tuple)

    if top_rows:
        db_rows = db_rows[:top_rows]

    if len(expected_rows) != len(db_rows):
        print(f'Number of rows are not equal: {len(expected_rows)} expected, db has {len(db_rows)}')
        return False

    return expected_rows == db_rows


def _query_result_to_rows(dbi, query):
    rows = dbi.execute_query(query)

    def _format(value):
        if isinstance(value, datetime.datetime):
            string_value = str(value)
            if timezone_regex.match(string_value):
                # Removing TZ info in string
                # (running tests in UTC timezone so no need to transform)
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


def _table_to_rows(dbi, table):
    query = f"""
       SELECT * FROM {table}
    """
    rows = dbi.execute_query(query)
    # strip columns that are not required for validation
    if 'L0' in table:
        rows = [r[4:] for r in rows]
    elif 'L1' in table:
        rows = [r[2:] for r in rows]

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


def _row_to_sortable_tuple(row):
    return tuple(('**none**' if e is None else e) for e in row)


def assert_dfs_equal_ignore_dtype(df1, df2):
    df1 = _sort_columns(df1)
    df2 = _sort_columns(df2)
    pd.util.testing.assert_frame_equal(df1, df2, check_dtype=False, check_index_type=False)


def _sort_columns(df):
    return df[sorted(df.columns)]
