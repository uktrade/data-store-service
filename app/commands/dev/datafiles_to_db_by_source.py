import click
from flask import current_app as app
from flask.cli import with_appcontext

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_world_bank_bound_rates import WorldBankBoundRatesPipeline
from app.etl.etl_world_bank_tariff import (
    WorldBankTariffBulkPipeline,
    WorldBankTariffPipeline,
    WorldBankTariffTestPipeline,
    WorldBankTariffTransformBulkPipeline,
    WorldBankTariffTransformPipeline,
    WorldBankTariffTransformTestPipeline,
)
from app.etl.manager import Manager as PipelineManager

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
    WorldBankTariffPipeline.data_source: [
        (DITEUCountryMembershipPipeline, 'dit/eu_country_membership'),
        (ComtradeCountryCodeAndISOPipeline, 'comtrade/country_code_and_iso'),
        (WorldBankBoundRatesPipeline, 'world_bank/bound_rates'),
        (WorldBankTariffPipeline, 'world_bank/tariff'),
        (WorldBankTariffTransformPipeline, None),
    ],
    WorldBankTariffTestPipeline.data_source: [
        (WorldBankTariffTestPipeline, 'world_bank/test'),
        (WorldBankTariffTransformTestPipeline, None),
    ],
    WorldBankTariffBulkPipeline.data_source: [
        (WorldBankTariffBulkPipeline, 'world_bank/bulk'),
        (WorldBankTariffTransformBulkPipeline, None),
    ],
}

exclude_pipelines = [
    WorldBankTariffTestPipeline.data_source,
    WorldBankTariffBulkPipeline.data_source,
]


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
            if (kwargs['all'] and _arg not in exclude_pipelines) or kwargs[arg]:
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
