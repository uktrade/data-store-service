import os

import boto3
import click


def delete_object(bucket_name, client, key):
    return client.delete_object(Bucket=bucket_name, Key=key)


def list_objects(bucket_name, client):
    objects = client.list_objects(Bucket=bucket_name)
    return objects


def move_object(bucket_name, client, key_from, key_to):
    copy_source = {'Bucket': bucket_name, 'Key': key_from}
    output = client.copy(CopySource=copy_source, Bucket=bucket_name, Key=key_to)
    client.delete_object(Bucket=bucket_name, Key=key_from)
    return output


def upload_object(bucket_name, client, path_from, path_to):
    return client.upload_file(path_from, bucket_name, path_to)


@click.group('s3', help="CRUD commands for s3")
def s3():
    pass


@s3.command(help='Delete s3 object')
@click.argument('key')
def delete(key):
    click.echo(f'{boto3}')
    bucket_name = os.environ['AWS_BUCKET_NAME']
    s3_client = boto3.client('s3')
    if not (key):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        click.echo(f'Deleting object {key} from s3 bucket, {bucket_name}')
        delete_object(bucket_name, s3_client, key.strip())


@s3.command(help='List objects in the data-store-service s3 bucket')
def ls():
    bucket_name = os.environ['AWS_BUCKET_NAME']
    s3_client = boto3.client('s3')
    click.echo(f'Listing objects form s3 bucket, {bucket_name}')
    objects = list_objects(bucket_name, s3_client)
    if 'Contents' in objects:
        for o in objects['Contents']:
            click.echo(f'\t{o["Key"]}')
    else:
        click.echo('No objects found.n')


@s3.command(help='Move s3 objects in the data-store-service bucket')
@click.argument('source')
@click.argument('destination')
def move(source, destination):
    bucket_name = os.environ['AWS_BUCKET_NAME']
    s3_client = boto3.client('s3')
    if not any([source, destination]):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        click.echo(f'Moving object from {source} to {destination}')
        move_object(bucket_name, s3_client, source, destination)


@s3.command(help='Upload local file to the data-store-service bucket')
@click.argument('input_filepath')
@click.argument('key')
def upload(input_filepath, key):
    bucket_name = os.environ['AWS_BUCKET_NAME']
    s3_client = boto3.client('s3')
    if not any([input_filepath, key]):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        click.echo(f'Uploading objects from {input_filepath} to {bucket_name}/{key}')
        upload_object(bucket_name, s3_client, input_filepath, key)
