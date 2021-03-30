import os
import uuid
from typing import List, Set

from data_engineering.common.sso.token import login_required
from flask import abort, redirect, render_template, request, url_for
from flask import current_app as app
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest

from app.constants import DataUploaderDataTypes, DataUploaderFileState, YES
from app.db.models.internal import Pipeline, PipelineDataFile
from app.uploader import forms
from app.uploader.csv_parser import CSVParser
from app.uploader.utils import (
    check_for_reserved_column_names,
    delete_file,
    process_pipeline_data_file,
    upload_file,
)

templates_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')

uploader_views = Blueprint(
    name="uploader_views",
    import_name=__name__,
    url_prefix='/upload',
    template_folder=templates_folder,
)


def render_uploader_template(*args, **kwargs):
    kwargs['active_menu'] = 'upload'
    return render_template(*args, **kwargs)


def get_object_or_404(model, **kwargs):
    obj = model.query.filter_by(**kwargs).first()
    if not obj:
        abort(404)
    return obj


@uploader_views.route('/', methods=('GET', 'POST'))
@login_required
def pipeline_select():
    show_form = False
    first_pipeline = Pipeline.query.first()
    if first_pipeline:
        show_form = True
    form = forms.PipelineSelectForm()
    if form.validate_on_submit():
        return redirect(
            url_for('uploader_views.pipeline_data_upload', slug=form.pipeline.data.slug)
        )
    return render_uploader_template('pipeline_select.html', form=form, show_form=show_form)


@uploader_views.route('/create/', methods=('GET', 'POST'))
@login_required
def pipeline_create():
    form = forms.PipelineForm()
    if form.validate_on_submit():
        pipeline = Pipeline(
            dataset=form.format(form.dataset.data),
            organisation=form.format(form.organisation.data),
        )
        pipeline.save()
        return redirect(url_for('uploader_views.pipeline_created', slug=pipeline.slug))
    return render_uploader_template('pipeline_create.html', heading='Create pipeline', form=form)


@uploader_views.route('/created/<slug>/')
@login_required
def pipeline_created(slug):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    return render_uploader_template('pipeline_created.html', pipeline=pipeline)


@uploader_views.route('/data/<slug>/', methods=('GET', 'POST'))
@login_required
def pipeline_data_upload(slug):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    form = forms.DataFileForm()
    if form.validate_on_submit():
        upload_folder = app.config['s3']['upload_folder']
        data_file_url = (
            f'{upload_folder}/{pipeline.organisation}/' f'{pipeline.dataset}/{uuid.uuid4()}'
        )
        data_file = PipelineDataFile(
            data_file_url=data_file_url,
            pipeline=pipeline,
            state=DataUploaderFileState.UPLOADING.value,
        )
        try:
            upload_file(data_file_url, form.csv_file.data)
            data_file.state = DataUploaderFileState.UPLOADED.value
            data_file.save()
        except Exception as e:
            data_file.state = DataUploaderFileState.FAILED.value
            data_file.error_message = str(e)
            data_file.save()
            raise
        return redirect(
            url_for('uploader_views.pipeline_data_verify', slug=pipeline.slug, file_id=data_file.id)
        )

    return render_uploader_template(
        'pipeline_data_upload.html', pipeline=pipeline, form=form, heading='Upload data',
    )


def get_missing_headers(current_version: List, new_version: List) -> Set:
    current_headers = set(header for header, _, _ in current_version)
    new_headers = set(header for header, _, _ in new_version)

    return current_headers - new_headers


@uploader_views.route('/data/<slug>/verify/<file_id>/', methods=('GET', 'POST'))
@login_required
def pipeline_data_verify(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(PipelineDataFile, pipeline=pipeline, id=file_id)

    form = forms.VerifyDataFileForm()
    is_form_valid = form.validate_on_submit()
    if is_form_valid and form.proceed.data != YES:
        pipeline_data_file.delete()
        delete_file(pipeline_data_file)
        return redirect(url_for('uploader_views.pipeline_select'))

    new_file_contents, new_file_err = CSVParser.get_csv_sample(
        pipeline_data_file.data_file_url, pipeline_data_file.delimiter, pipeline_data_file.quote
    )

    current_file_contents, current_column_types, missing_headers = None, None, set()
    if pipeline.latest_version:
        data_file_latest = pipeline.latest_version
        current_file_contents, current_file_err = CSVParser.get_csv_sample(
            data_file_latest.data_file_url, data_file_latest.delimiter, data_file_latest.quote
        )
        missing_headers = get_missing_headers(
            current_version=current_file_contents, new_version=new_file_contents
        )
        current_column_types = dict(data_file_latest.column_types)

    if is_form_valid:
        selected_column_types = [
            (column, request.form[column]) for column, _, _ in new_file_contents
        ]
        pipeline_data_file.column_types = selected_column_types
        pipeline_data_file.state = DataUploaderFileState.VERIFIED.value
        pipeline_data_file.save()

        thread = process_pipeline_data_file(pipeline_data_file)
        thread.start()

        return redirect(
            url_for(
                'uploader_views.pipeline_data_uploaded',
                slug=pipeline.slug,
                file_id=pipeline_data_file.id,
            )
        )

    if new_file_err is None:
        uploaded_columns = set(x[0] for x in new_file_contents)
        error_message = check_for_reserved_column_names(pipeline_data_file, uploaded_columns)
        if error_message:
            new_file_contents = None
            new_file_err = error_message

    if new_file_err is not None:
        pipeline_data_file.state = DataUploaderFileState.FAILED.value
        pipeline_data_file.error_message = new_file_err
        pipeline_data_file.save()
        form.errors['non_field_errors'] = [new_file_err]

    return render_uploader_template(
        'pipeline_data_verify.html',
        pipeline=pipeline,
        new_file_contents=new_file_contents,
        current_file_contents=current_file_contents,
        current_column_types=current_column_types,
        data_types=DataUploaderDataTypes.values(),
        format_row_data=format_row_data,
        form=form,
        missing_headers=missing_headers,
    )


@uploader_views.route('/data/<slug>/restore/<file_id>/', methods=('GET', 'POST'))
def pipeline_restore_version(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    form = forms.RestoreVersionForm()
    is_form_valid = form.validate_on_submit()
    if is_form_valid and form.proceed.data != YES:
        return redirect(url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug,))

    data_file_latest = pipeline.latest_version
    file_contents_latest, _ = CSVParser.get_csv_sample(
        data_file_latest.data_file_url, data_file_latest.delimiter, data_file_latest.quote
    )

    data_file_to_restore = get_object_or_404(PipelineDataFile, pipeline=pipeline, id=file_id)
    file_contents_to_restore, _ = CSVParser.get_csv_sample(
        data_file_to_restore.data_file_url,
        data_file_to_restore.delimiter,
        data_file_to_restore.quote,
    )

    if is_form_valid:
        data_file_to_restore.state = DataUploaderFileState.VERIFIED.value
        data_file_to_restore.save()

        thread = process_pipeline_data_file(data_file_to_restore)
        thread.start()

        return redirect(
            url_for(
                'uploader_views.pipeline_data_uploaded',
                slug=pipeline.slug,
                file_id=data_file_to_restore.id,
            )
        )

    return render_uploader_template(
        'pipeline_restore_version.html',
        form=form,
        file_contents_latest=file_contents_latest,
        file_contents_to_restore=file_contents_to_restore,
        column_types_latest=dict(data_file_latest.column_types),
        column_types_to_restore=dict(data_file_to_restore.column_types),
        format_row_data=format_row_data,
    )


def format_row_data(row):
    return ", ".join(row)


@uploader_views.route('/data/<slug>/uploaded/<file_id>/')
@login_required
def pipeline_data_uploaded(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(PipelineDataFile, pipeline=pipeline, id=file_id)
    schema_parts = pipeline.pipeline_schema.split('.')
    if len(schema_parts) != 2:
        raise BadRequest("Invalid schema for this pipeline.")
    data_workspace_schema_name = schema_parts[0]
    data_workspace_table_name = schema_parts[1]
    file_id = pipeline_data_file.id

    if pipeline_data_file.state == DataUploaderFileState.FAILED.value:
        return redirect(
            url_for('uploader_views.pipeline_data_upload_failed', slug=slug, file_id=file_id)
        )

    return render_uploader_template(
        'pipeline_data_uploaded.html',
        pipeline=pipeline,
        file_id=file_id,
        data_workspace_schema_name=data_workspace_schema_name,
        data_workspace_table_name=data_workspace_table_name,
    )


@uploader_views.route('/progress/<file_id>/')
@login_required
def progress(file_id):
    file = get_object_or_404(PipelineDataFile, id=file_id)
    state = file.state
    response = {'state': state}
    if not state:
        response['progress'] = '0'
        response['info'] = 'Starting new data upload'
    elif state == DataUploaderFileState.UPLOADING.value:
        response['progress'] = '10'
        response['info'] = 'Uploading data'
    elif state == DataUploaderFileState.UPLOADED.value:
        response['progress'] = '20'
        response['info'] = 'Data successfully uploaded'
    elif state == DataUploaderFileState.VERIFIED.value:
        response['progress'] = '30'
        response['info'] = 'Data successfully verified'
    elif state == DataUploaderFileState.PROCESSING_DSS.value:
        response['progress'] = '40'
        response['info'] = 'Data is being processed by Data Store Service'
    elif state == DataUploaderFileState.PROCESSING_DATAFLOW.value:
        response['progress'] = '70'
        response['info'] = 'Data is being processed by Data Flow'
    elif state == DataUploaderFileState.COMPLETED.value:
        response['progress'] = '100'
        response['info'] = 'Data successfully processed'
    elif state == DataUploaderFileState.FAILED.value:
        response['progress'] = '100'
        response['info'] = 'Sorry, there is a problem with the service. Try again later.'
    return response


@uploader_views.route('/data/<slug>/uploaded/<file_id>/failed/')
@login_required
def pipeline_data_upload_failed(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(PipelineDataFile, pipeline=pipeline, id=file_id)
    schema_parts = pipeline.pipeline_schema.split('.')
    if len(schema_parts) != 2:
        raise BadRequest("Invalid schema for this pipeline.")
    data_workspace_schema_name = schema_parts[0]
    data_workspace_table_name = schema_parts[1]
    file_id = pipeline_data_file.id
    return render_uploader_template(
        'pipeline_data_upload_failed.html',
        pipeline=pipeline,
        file_id=file_id,
        data_workspace_schema_name=data_workspace_schema_name,
        data_workspace_table_name=data_workspace_table_name,
    )
