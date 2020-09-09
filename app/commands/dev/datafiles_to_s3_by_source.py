import os

import click
from datatools.io.storage import S3Storage
from flask import current_app as app
from flask.cli import with_appcontext

from app.downloader.manager import Manager as DownloadManager
from app.downloader.web.companies_house import CompaniesHouseAccounts

arg_to_downloader_info_list = {
    'companies_house.accounts': (CompaniesHouseAccounts, 'companies_house/accounts/')
}


@click.command('datafiles_to_s3_by_source')
@click.option('--all', is_flag=True, help='download all data sources to s3')
@with_appcontext
def datafiles_to_s3_by_source(**kwargs):
    """
    Download datafiles to s3 bucket
    """
    if not any(kwargs.values()):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        bucket = app.config['s3']['bucket_url']
        manager = DownloadManager()
        for _arg, info_list in arg_to_downloader_info_list.items():
            arg = _arg.replace(".", "__")
            if (kwargs['all'] and arg) or kwargs[arg]:
                downloader = info_list[0]
                sub_path = info_list[1]
                path = os.path.join(bucket, app.config['s3']['datasets_folder'], sub_path)
                storage = S3Storage(path)
                manager.register(downloader(storage))
        manager.update_datasources()


def _data_source_option(option_name):
    return click.option(
        f'--{option_name}',
        f'{option_name.replace(".", "__")}',
        is_flag=True,
        help=f'{option_name} data download',
    )


for k in arg_to_downloader_info_list.keys():
    datafiles_to_s3_by_source = _data_source_option(k)(datafiles_to_s3_by_source)
