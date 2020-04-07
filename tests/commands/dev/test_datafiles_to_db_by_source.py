from unittest import mock

import pytest

from app.commands.dev.datafiles_to_db_by_source import (
    ComtradeCountryCodeAndISOPipeline,
    datafiles_to_db_by_source,
    DITBACIPipeline,
    DITEUCountryMembershipPipeline,
    DITReferencePostcodesPipeline,
    ONSPostcodeDirectoryPipeline,
    WorldBankBoundRatesPipeline,
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline,
)


class TestDataFileToDBBySource:
    def test_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(datafiles_to_db_by_source)
        assert 'Populate tables with source files' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @pytest.mark.parametrize(
        'args,pipeline_register_calls,process_called,expected_msg',
        (
            (
                ['--all'],
                [
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'ons/postcode_directory/',
                        'pipeline': ONSPostcodeDirectoryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/baci',
                        'pipeline': DITBACIPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/eu_country_membership',
                        'pipeline': DITEUCountryMembershipPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/reference_postcodes',
                        'pipeline': DITReferencePostcodesPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/tariff',
                        'pipeline': WorldBankTariffPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'comtrade/country_code_and_iso',
                        'pipeline': ComtradeCountryCodeAndISOPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/bound_rates',
                        'pipeline': WorldBankBoundRatesPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': None,
                        'pipeline': WorldBankTariffTransformPipeline,
                    },
                ],
                True,
                None,
            ),
            (
                ['--world_bank.tariff'],
                [
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/baci',
                        'pipeline': DITBACIPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/eu_country_membership',
                        'pipeline': DITEUCountryMembershipPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'comtrade/country_code_and_iso',
                        'pipeline': ComtradeCountryCodeAndISOPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/bound_rates',
                        'pipeline': WorldBankBoundRatesPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/tariff',
                        'pipeline': WorldBankTariffPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': None,
                        'pipeline': WorldBankTariffTransformPipeline,
                    },
                ],
                True,
                None,
            ),
            (
                ['--ons.postcode_directory', '--force'],
                [
                    {
                        'continue_transform': False,
                        'force': True,
                        'products': None,
                        'sub_directory': 'ons/postcode_directory/',
                        'pipeline': ONSPostcodeDirectoryPipeline,
                    },
                ],
                True,
                None,
            ),
            (
                ['--world_bank.tariff', '--continue'],
                [
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/baci',
                        'pipeline': DITBACIPipeline,
                    },
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/eu_country_membership',
                        'pipeline': DITEUCountryMembershipPipeline,
                    },
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': 'comtrade/country_code_and_iso',
                        'pipeline': ComtradeCountryCodeAndISOPipeline,
                    },
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/bound_rates',
                        'pipeline': WorldBankBoundRatesPipeline,
                    },
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': 'world_bank/tariff',
                        'pipeline': WorldBankTariffPipeline,
                    },
                    {
                        'continue_transform': True,
                        'force': False,
                        'products': None,
                        'sub_directory': None,
                        'pipeline': WorldBankTariffTransformPipeline,
                    },
                ],
                True,
                None,
            ),
            (
                ['--world_bank.tariff', '--products', '1234,5623'],
                [
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': 'dit/baci',
                        'pipeline': DITBACIPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': 'dit/eu_country_membership',
                        'pipeline': DITEUCountryMembershipPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': 'comtrade/country_code_and_iso',
                        'pipeline': ComtradeCountryCodeAndISOPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': 'world_bank/bound_rates',
                        'pipeline': WorldBankBoundRatesPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': 'world_bank/tariff',
                        'pipeline': WorldBankTariffPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': '1234,5623',
                        'sub_directory': None,
                        'pipeline': WorldBankTariffTransformPipeline,
                    },
                ],
                True,
                None,
            ),
            ([], None, False, 'Populate tables with source file'),
        ),
    )
    @mock.patch('app.etl.manager.Manager.pipeline_register')
    @mock.patch('app.etl.manager.Manager.pipeline_process_all')
    def test_cmd(
        self,
        mock_process_all,
        mock_pipeline_register,
        app_with_db,
        args,
        pipeline_register_calls,
        process_called,
        expected_msg,
    ):
        mock_pipeline_register.return_value = None
        mock_process_all.return_value = None
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(datafiles_to_db_by_source, args)

        if pipeline_register_calls:
            assert mock_pipeline_register.called is True
            for parameters in pipeline_register_calls:
                mock_pipeline_register.assert_any_call(**parameters)
            assert mock_pipeline_register.call_count is len(pipeline_register_calls)
        else:
            assert mock_pipeline_register.called is False
        assert mock_process_all.called is process_called
        if expected_msg:
            assert expected_msg in result.output
