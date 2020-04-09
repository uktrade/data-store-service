from unittest import mock

import pytest

from app.commands.dev.db import db


class TestDbCommand:
    def test_db_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(db)
        assert 'Create/Drop database or database tables' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @pytest.mark.parametrize(
        'drop_database,create_tables,drop_tables,create_database,recreate_tables,expected_msg',
        (
            (False, False, False, False, False, True),
            (True, False, False, False, False, False),
            (False, True, False, False, False, False),
            (False, False, True, False, False, False),
            (False, False, False, True, False, False),
            (False, False, False, False, True, False),
        ),
    )
    @mock.patch('flask_migrate.upgrade')
    @mock.patch('data_engineering.common.db.dbi.DBI.create_schema')
    @mock.patch('data_engineering.common.db.dbi.DBI.drop_schema')
    @mock.patch('sqlalchemy_utils.create_database')
    @mock.patch('sqlalchemy_utils.drop_database')
    def test_run_db(
        self,
        mock_drop_database,
        mock_create_database,
        mock_drop_tables,
        mock_create_schemas,
        mock_run_migrations,
        drop_database,
        create_tables,
        drop_tables,
        create_database,
        recreate_tables,
        expected_msg,
        app_with_db,
    ):
        mock_drop_database.return_value = None
        mock_create_database.return_value = None
        mock_drop_tables.return_value = None
        mock_create_schemas.return_value = None
        mock_run_migrations.return_value = None
        runner = app_with_db.test_cli_runner()

        args = []
        if drop_tables:
            args.append('--drop_tables')
        if create_tables:
            args.append('--create_tables')
        if drop_database:
            args.append('--drop')
        if create_database:
            args.append('--create')
        if recreate_tables:
            args.append('--recreate_tables')

        result = runner.invoke(db, args)
        assert mock_drop_database.called is drop_database
        assert mock_create_database.called is create_database
        assert mock_drop_tables.called is any([drop_tables, recreate_tables])
        assert mock_create_schemas.called is any([create_tables, create_database, recreate_tables])
        assert mock_run_migrations.called is any([create_tables, create_database, recreate_tables])

        if expected_msg:
            assert result.output.startswith('Usage: db [OPTIONS]')
