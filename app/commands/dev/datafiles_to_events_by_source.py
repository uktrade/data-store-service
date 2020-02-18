import click
from flask import current_app as app
from flask.cli import with_appcontext

from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_ons_reference_postcode_directory import ONSReferencePostcodeDirectoryPipeline
from app.etl.manager import Manager as PipelineManager


arg_to_pipeline_config_list = {
    # format:  {'command option': [(pipeline, dataset subdir)]}
    # pipeline processing is grouped by data_source,
    # except when multiple format versions are supported or
    # pipelines are dependent on other pipelines
    ONSPostcodeDirectoryPipeline.data_source: [
        (ONSPostcodeDirectoryPipeline, 'ons/postcode_directory/')
    ],
    ONSReferencePostcodeDirectoryPipeline.data_source: [
        (ONSReferencePostcodeDirectoryPipeline, 'data_workspace/reference_postcodes')
    ],
}


@click.command('datafiles_to_events_by_source')
@with_appcontext
@click.option('--all', is_flag=True, help='all datafiles to events')
def datafiles_to_events_by_source(**kwargs):
    """
    Populate tables with source files
    """
    manager = PipelineManager(storage=app.config['inputs']['source-folder'], dbi=app.dbi)
    for arg, pipeline_info_list in arg_to_pipeline_config_list.items():
        arg = arg.replace(".", "__")
        if kwargs['all'] or kwargs[arg]:
            for pipeline, sub_dir in pipeline_info_list:
                manager.pipeline_register(pipeline=pipeline, sub_directory=sub_dir)
    manager.pipeline_process_all()


def _pipeline_option(option_name):
    return click.option(
        f'--{option_name}',
        f'{option_name.replace(".", "__")}',
        is_flag=True,
        help=f'{option_name} data to events',
    )


for k in arg_to_pipeline_config_list.keys():
    datafiles_to_events_by_source = _pipeline_option(k)(datafiles_to_events_by_source)
