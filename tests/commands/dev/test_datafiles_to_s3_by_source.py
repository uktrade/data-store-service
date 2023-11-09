from unittest import mock

import pytest

from app.commands.dev.datafiles_to_s3_by_source import datafiles_to_s3_by_source


class TestDownloadToS3BySource:
    def test_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(datafiles_to_s3_by_source)
        assert 'Download datafiles to s3' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @pytest.mark.parametrize(
        'args,datasource_register_call_count,process_called,expected_msg',
        (
            (
                ['--all'],
                3,
                True,
                None,
            ),
            (
                ['--companies_house.accounts_legacy'],
                1,
                True,
                None,
            ),
            (
                ['--hmrc.exporters'],
                1,
                True,
                None,
            ),
            (
                ['--ons.postcode_directory'],
                1,
                True,
                None,
            ),
            ([], None, False, 'Download datafiles to s3 bucket'),
        ),
    )
    @mock.patch('app.downloader.manager.Manager.register')
    @mock.patch('app.downloader.manager.Manager.update_datasources')
    def test_cmd(
        self,
        mock_update_datasources,
        mock_register_datasource,
        app_with_db,
        args,
        datasource_register_call_count,
        process_called,
        expected_msg,
    ):
        mock_register_datasource.return_value = None
        mock_update_datasources.return_value = None
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(datafiles_to_s3_by_source, args)

        if datasource_register_call_count:
            assert mock_register_datasource.called is True
            assert mock_register_datasource.call_count is datasource_register_call_count
        else:
            assert mock_register_datasource.called is False
        assert mock_update_datasources.called is process_called
        if expected_msg:
            assert expected_msg in result.output
