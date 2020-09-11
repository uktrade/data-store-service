from unittest import mock

import pytest

from app.commands.dev.datafiles_to_db_by_source import (
    ComtradeCountryCodeAndISOPipeline,
    datafiles_to_db_by_source,
    DITBACIPipeline,
    DITEUCountryMembershipPipeline,
    DITReferencePostcodesPipeline,
    ONSPostcodeDirectoryPipeline,
    SPIREApplicationAmendmentPipeline,
    SPIREApplicationCountryPipeline,
    SPIREApplicationPipeline,
    SPIREArsPipeline,
    SPIREBatchPipeline,
    SPIREControlEntryPipeline,
    SPIRECountryGroupEntryPipeline,
    SPIRECountryGroupPipeline,
    SPIREEndUserPipeline,
    SPIREFootnoteEntryPipeline,
    SPIREFootnotePipeline,
    SPIREGoodsIncidentPipeline,
    SPIREIncidentPipeline,
    SPIREMediaFootnoteCountryPipeline,
    SPIREMediaFootnoteDetailPipeline,
    SPIREMediaFootnotePipeline,
    SPIREOglTypePipeline,
    SPIREReasonForRefusalPipeline,
    SPIRERefArsSubjectPipeline,
    SPIRERefCountryMappingPipeline,
    SPIRERefDoNotReportValuePipeline,
    SPIRERefReportRatingPipeline,
    SPIREReturnPipeline,
    SPIREThirdPartyPipeline,
    SPIREUltimateEndUserPipeline,
    WorldBankBoundRatesPipeline,
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline,
)
from app.etl.organisation.companies_house import CompaniesHouseAccountsPipeline


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
                        'sub_directory': 'dit/spire/application',
                        'pipeline': SPIREApplicationPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/application_amendment',
                        'pipeline': SPIREApplicationAmendmentPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/application_country',
                        'pipeline': SPIREApplicationCountryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ars',
                        'pipeline': SPIREArsPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/batch',
                        'pipeline': SPIREBatchPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/control_entry',
                        'pipeline': SPIREControlEntryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/country_group',
                        'pipeline': SPIRECountryGroupPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/country_group_entry',
                        'pipeline': SPIRECountryGroupEntryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/end_user',
                        'pipeline': SPIREEndUserPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/footnote',
                        'pipeline': SPIREFootnotePipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/footnote_entry',
                        'pipeline': SPIREFootnoteEntryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/goods_incident',
                        'pipeline': SPIREGoodsIncidentPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/incident',
                        'pipeline': SPIREIncidentPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/media_footnote',
                        'pipeline': SPIREMediaFootnotePipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/media_footnote_country',
                        'pipeline': SPIREMediaFootnoteCountryPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/media_footnote_detail',
                        'pipeline': SPIREMediaFootnoteDetailPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ogl_type',
                        'pipeline': SPIREOglTypePipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/reason_for_refusal',
                        'pipeline': SPIREReasonForRefusalPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ref_ars_subject',
                        'pipeline': SPIRERefArsSubjectPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ref_country_mapping',
                        'pipeline': SPIRERefCountryMappingPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ref_do_not_report_value',
                        'pipeline': SPIRERefDoNotReportValuePipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ref_report_rating',
                        'pipeline': SPIRERefReportRatingPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/return',
                        'pipeline': SPIREReturnPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/third_party',
                        'pipeline': SPIREThirdPartyPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/spire/ultimate_end_user',
                        'pipeline': SPIREUltimateEndUserPipeline,
                    },
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'unpack': False,
                        'trigger_dataflow_dag': True,
                        'sub_directory': 'companies_house/accounts/',
                        'pipeline': CompaniesHouseAccountsPipeline,
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
                ],
                True,
                None,
            ),
            (
                ['--dit.baci'],
                [
                    {
                        'continue_transform': False,
                        'force': False,
                        'products': None,
                        'sub_directory': 'dit/baci',
                        'pipeline': DITBACIPipeline,
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
                ['--world_bank.tariff', '--continue', '--transform'],
                [
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
                        'required_flag': 'transform',
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
                ['--world_bank.tariff', '--products', '1234,5623', '--transform'],
                [
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
                        'required_flag': 'transform',
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
            (
                ['--companies_house.accounts', '--force'],
                [
                    {
                        'continue_transform': False,
                        'force': True,
                        'products': None,
                        'unpack': False,
                        'trigger_dataflow_dag': True,
                        'sub_directory': 'companies_house/accounts/',
                        'pipeline': CompaniesHouseAccountsPipeline,
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
