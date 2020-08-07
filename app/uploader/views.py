import os
import uuid

from data_engineering.common.sso.token import login_required
from flask import abort, redirect, render_template, url_for
from flask import current_app as app
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest

from app.constants import DataUploaderFileState, YES
from app.db.models.internal import Pipeline, PipelineDataFile
from app.uploader import forms
from app.uploader.utils import (
    delete_file,
    get_column_types,
    get_s3_file_sample,
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
            column_types=[['temp', 'text']],  # temporary placeholder
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


def get_missing_headers_compared_to_previous_data(file_contents, pipeline):
    if len(pipeline.data_files) < 2:
        return set()

    previous_data_file = pipeline.data_files[-2]

    previous_file_contents, err = get_s3_file_sample(
        previous_data_file.data_file_url, pipeline.delimiter, pipeline.quote
    )

    new_file_headers = set(str(k) for k in file_contents.to_dict().keys())
    previous_file_headers = set(str(k) for k in previous_file_contents.to_dict().keys())

    return previous_file_headers - new_file_headers


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

    file_contents, err = get_s3_file_sample(
        pipeline_data_file.data_file_url, pipeline.delimiter, pipeline.quote
    )

    missing_headers = get_missing_headers_compared_to_previous_data(file_contents, pipeline)

    if is_form_valid:
        pipeline.column_types = get_column_types(file_contents)
        pipeline.save()
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

    elif not file_contents.empty and not err:
        file_contents = file_contents.to_dict()
    else:
        pipeline_data_file.state = DataUploaderFileState.FAILED.value
        pipeline_data_file.error_message = err
        pipeline_data_file.save()
        form.errors['non_field_errors'] = [f'Unable to process file - {err}']
    return render_uploader_template(
        'pipeline_data_verify.html',
        pipeline=pipeline,
        file_contents=file_contents,
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
    file_contents_latest, _ = get_s3_file_sample(
        data_file_latest.data_file_url, pipeline.delimiter, pipeline.quote
    )

    data_file_to_restore = get_object_or_404(PipelineDataFile, pipeline=pipeline, id=file_id)
    file_contents_to_restore, _ = get_s3_file_sample(
        data_file_to_restore.data_file_url, pipeline.delimiter, pipeline.quote
    )

    if is_form_valid:
        pipeline.column_types = get_column_types(file_contents_to_restore)
        pipeline.save()
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

    if not file_contents_latest.empty:
        file_contents_latest = file_contents_latest.to_dict()

    if not file_contents_to_restore.empty:
        file_contents_to_restore = file_contents_to_restore.to_dict()

    return render_uploader_template(
        'pipeline_restore_version.html',
        form=form,
        file_contents_latest=file_contents_latest,
        file_contents_to_restore=file_contents_to_restore,
        format_row_data=format_row_data,
    )


def format_row_data(row):
    return ", ".join(row.values())


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
        response[
            'info'
        ] = f'Oops, something went wrong{": " + file.error_message if file.error_message else ""}'
    return response
