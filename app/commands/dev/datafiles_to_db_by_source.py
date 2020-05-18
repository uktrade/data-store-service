import click
from flask import current_app as app
from flask.cli import with_appcontext

from app.etl.manager import Manager as PipelineManager
from app.etl.organisation.comtrade import ComtradeCountryCodeAndISOPipeline
from app.etl.organisation.dit import (
    DITBACIPipeline,
    DITEUCountryMembershipPipeline,
    DITReferencePostcodesPipeline,
)
from app.etl.organisation.ons import ONSPostcodeDirectoryPipeline
from app.etl.organisation.spire import (
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
    SPIREPipeline,
    SPIREReasonForRefusalPipeline,
    SPIRERefArsSubjectPipeline,
    SPIRERefCountryMappingPipeline,
    SPIRERefDoNotReportValuePipeline,
    SPIRERefReportRatingPipeline,
    SPIREReturnPipeline,
    SPIREThirdPartyPipeline,
    SPIREUltimateEndUserPipeline,
)
from app.etl.organisation.world_bank import (
    WorldBankBoundRatesPipeline,
    WorldBankTariffPipeline,
    WorldBankTariffTransformPipeline,
)

arg_to_pipeline_config_list = {
    # format:  {'command option': [(pipeline, dataset subdir)]}
    # pipeline processing is grouped by data_source,
    # except when multiple format versions are supported or
    # pipelines are dependent on other pipelines
    ONSPostcodeDirectoryPipeline.data_source: [
        (ONSPostcodeDirectoryPipeline, 'ons/postcode_directory/')
    ],
    DITReferencePostcodesPipeline.data_source: [
        (DITReferencePostcodesPipeline, 'dit/reference_postcodes')
    ],
    DITBACIPipeline.data_source: [(DITBACIPipeline, 'dit/baci')],
    WorldBankTariffPipeline.data_source: [
        (DITEUCountryMembershipPipeline, 'dit/eu_country_membership'),
        (ComtradeCountryCodeAndISOPipeline, 'comtrade/country_code_and_iso'),
        (WorldBankBoundRatesPipeline, 'world_bank/bound_rates'),
        (WorldBankTariffPipeline, 'world_bank/tariff'),
        (WorldBankTariffTransformPipeline, None),
    ],
    SPIREPipeline.data_source: [
        (SPIREBatchPipeline, 'dit/spire/batch'),
        (SPIRECountryGroupPipeline, 'dit/spire/country_group'),
        (SPIREEndUserPipeline, 'dit/spire/end_user'),
        (SPIREFootnotePipeline, 'dit/spire/footnote'),
        (SPIREMediaFootnotePipeline, 'dit/spire/media_footnote'),
        (SPIREMediaFootnoteCountryPipeline, 'dit/spire/media_footnote_country'),
        (SPIREOglTypePipeline, 'dit/spire/ogl_type'),
        (SPIRERefArsSubjectPipeline, 'dit/spire/ref_ars_subject'),
        (SPIRERefCountryMappingPipeline, 'dit/spire/ref_country_mapping'),
        (SPIRERefDoNotReportValuePipeline, 'dit/spire/ref_do_not_report_value'),
        (SPIRERefReportRatingPipeline, 'dit/spire/ref_report_rating'),
        (SPIREApplicationPipeline, 'dit/spire/application'),
        (SPIRECountryGroupEntryPipeline, 'dit/spire/country_group_entry'),
        (SPIREMediaFootnoteDetailPipeline, 'dit/spire/media_footnote_detail'),
        (SPIREReturnPipeline, 'dit/spire/return'),
        (SPIREApplicationAmendmentPipeline, 'dit/spire/application_amendment'),
        (SPIREApplicationCountryPipeline, 'dit/spire/application_country'),
        (SPIREFootnoteEntryPipeline, 'dit/spire/footnote_entry'),
        (SPIREGoodsIncidentPipeline, 'dit/spire/goods_incident'),
        (SPIREIncidentPipeline, 'dit/spire/incident'),
        (SPIREThirdPartyPipeline, 'dit/spire/third_party'),
        (SPIREUltimateEndUserPipeline, 'dit/spire/ultimate_end_user'),
        (SPIREArsPipeline, 'dit/spire/ars'),
        (SPIREControlEntryPipeline, 'dit/spire/control_entry'),
        (SPIREReasonForRefusalPipeline, 'dit/spire/reason_for_refusal'),
    ],
}


@click.command('datafiles_to_db_by_source')
@with_appcontext
@click.option('--all', is_flag=True, help='ingest datafile into the DB')
@click.option('--force', is_flag=True, help='Force pipeline')
@click.option(
    '--continue', is_flag=True, help='Continue transform [World bank tariff pipelines ' 'only]'
)
@click.option(
    '--products',
    type=str,
    help='Only process selected products [World bank tariff ' 'pipelines only]',
    default=None,
)
def datafiles_to_db_by_source(**kwargs):
    """
    Populate tables with source files
    """

    if not any(kwargs.values()):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        manager = PipelineManager(storage=app.config['inputs']['source-folder'], dbi=app.dbi)
        for _arg, pipeline_info_list in arg_to_pipeline_config_list.items():
            arg = _arg.replace(".", "__")
            if (kwargs['all'] and _arg) or kwargs[arg]:
                for pipeline, sub_dir in pipeline_info_list:
                    manager.pipeline_register(
                        pipeline=pipeline,
                        sub_directory=sub_dir,
                        force=kwargs['force'],
                        continue_transform=kwargs['continue'],
                        products=kwargs['products'],
                    )
        manager.pipeline_process_all()


def _pipeline_option(option_name):
    return click.option(
        f'--{option_name}',
        f'{option_name.replace(".", "__")}',
        is_flag=True,
        help=f'{option_name} data ingestion',
    )


for k in arg_to_pipeline_config_list.keys():
    datafiles_to_db_by_source = _pipeline_option(k)(datafiles_to_db_by_source)
