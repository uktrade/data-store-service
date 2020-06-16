import os
from unittest import mock

import pytest

from app.commands.dev.dump_schema import (
    dump_schema,
    dump_table,
    get_table_names,
    make_csv_folder,
)


class TestDumpSchemaCommand:
    @pytest.mark.parametrize(
        'cmd_args,exit_code', (([], 0), (['public'], 2), (['--schema', 'public'], 2),),
    )
    def test_dump_schema_without_invalid_options(self, app_with_db, cmd_args, exit_code):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(dump_schema, cmd_args)
        assert 'Usage: dump_schema [OPTIONS]' in result.output
        assert result.exit_code == exit_code
        if not exit_code:
            assert result.exception is None
        else:
            assert result.exception is not None

    @mock.patch('os.makedirs')
    def test_make_csv_folder(self, mock_os_makedirs):
        mock_os_makedirs.return_value = None
        expected_folder = f'{os.getcwd()}/.csv/hello'
        actual_folder = make_csv_folder('hello')
        assert actual_folder == expected_folder
        mock_os_makedirs.assert_called_once_with(expected_folder, exist_ok=True)

    @mock.patch('subprocess.run')
    def test_dump_table(self, mock_subproces_run):
        mock_subproces_run.return_value = None
        dump_table('http://db_url', '/test_folder', 'test_schema', 'test_table')
        mock_subproces_run.assert_called_once_with(
            'psql http://db_url -c "\\COPY \\"test_schema\\".\\"test_table\\"'
            ' to \'/test_folder/TEST_TABLE.csv\' WITH (FORMAT CSV, HEADER, NULL \'NULL\')"',
            shell=True,
        )

    def test_get_table_names(self, app_with_db):
        tables = get_table_names('spire')
        assert sorted(tables) == [
            'application_amendments',
            'application_countries',
            'applications',
            'ars',
            'batches',
            'control_entries',
            'country_group_entries',
            'country_groups',
            'end_users',
            'footnote_entries',
            'footnotes',
            'goods_incidents',
            'incidents',
            'media_footnote_countries',
            'media_footnote_details',
            'media_footnotes',
            'ogl_types',
            'reasons_for_refusal',
            'ref_ars_subjects',
            'ref_country_mappings',
            'ref_do_not_report_values',
            'ref_report_ratings',
            'returns',
            'third_parties',
            'ultimate_end_users',
        ]

    @mock.patch('subprocess.run')
    @mock.patch('app.commands.dev.dump_schema.get_table_names')
    @mock.patch('os.makedirs')
    def test_dump_spire_schema(
        self, mock_os_makedirs, mock_get_table_names, mock_subprocess_run, app_with_db
    ):
        mock_os_makedirs.return_value = []
        mock_get_table_names.return_value = ['test']
        mock_subprocess_run.return_value = None
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(dump_schema, ['--schema', 'spire'])
        assert 'Creating TEST.csv' in result.output
