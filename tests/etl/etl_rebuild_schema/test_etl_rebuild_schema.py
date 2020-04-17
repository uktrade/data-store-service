from unittest import mock


from app.etl.etl_rebuild_schema import _create_table


class TestCreateTable:
    def test_if_drop_existing_true_drop_table(self):

        data_column_types = []
        dbi = mock.Mock()
        drop_existing = True
        table_name = 'table_name1'

        _create_table(data_column_types, dbi, table_name, drop_existing=drop_existing)

        dbi.drop_table.assert_called_once_with('table_name1')

    def test_if_drop_existing_false_dont_drop_table(self):

        data_column_types = []
        dbi = mock.Mock()
        drop_existing = False
        table_name = 'table_name1'

        _create_table(data_column_types, dbi, table_name, drop_existing=drop_existing)

        dbi.drop_table.assert_not_called()


class TestCreateTableIntegrationTests:
    def test_table_creation(self, app_with_db):

        data_column_types = [('my_int', 'integer'), ('my_text', 'text')]
        dbi = app_with_db.dbi
        drop_existing = False
        table_name = 'table_name1'

        _create_table(data_column_types, dbi, table_name, drop_existing=drop_existing)

        query = ''' select current_schema '''
        schema = dbi.execute_query(query)[0][0]
        query = f'''
        select
            count(1)

        from information_schema.tables

        where table_name = '{table_name}'
            and table_schema = '{schema}'
        '''
        n_rows = dbi.execute_query(query)[0][0]
        assert n_rows == 1
