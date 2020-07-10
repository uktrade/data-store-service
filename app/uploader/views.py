import os

from data_engineering.common.sso.token import login_required
from flask import abort, redirect, render_template, url_for
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest

from app.constants import YES
from app.db.models.internal import Pipeline, PipelineDataFile
from app.uploader.forms import DataFileForm, PipelineForm, PipelineSelectForm, VerifyDataFileForm
from app.uploader.utils import (
    get_s3_file_sample,
    process_pipeline_data_file,
    save_column_types,
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
    form = PipelineSelectForm()
    if form.validate_on_submit():
        return redirect(
            url_for('uploader_views.pipeline_data_upload', slug=form.pipeline.data.slug)
        )
    return render_uploader_template('pipeline_select.html', form=form, show_form=show_form)


@uploader_views.route('/create/', methods=('GET', 'POST'))
@login_required
def pipeline_create():
    form = PipelineForm()
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
    form = DataFileForm()
    if form.validate_on_submit():
        data_file_url = upload_file(form.csv_file.data, form.csv_file.data.filename, pipeline)
        data_file = PipelineDataFile(data_file_url=data_file_url, pipeline=pipeline)
        data_file.save()
        return redirect(
            url_for('uploader_views.pipeline_data_verify', slug=pipeline.slug, file_id=data_file.id)
        )
    return render_uploader_template(
        'pipeline_data_upload.html', pipeline=pipeline, form=form, heading='Upload data',
    )


@uploader_views.route('/data/<slug>/verify/<file_id>/', methods=('GET', 'POST'))
@login_required
def pipeline_data_verify(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, id=file_id, deleted=False
    )

    form = VerifyDataFileForm()
    is_form_valid = form.validate_on_submit()
    if is_form_valid and form.proceed.data != YES:
        pipeline_data_file.deleted = True
        pipeline_data_file.save()
        return redirect(url_for('uploader_views.pipeline_select'))

    file_contents = get_s3_file_sample(
        pipeline_data_file.data_file_url, pipeline.delimiter, pipeline.quote
    )
    if is_form_valid:
        save_column_types(pipeline, file_contents)
        return redirect(
            url_for(
                'uploader_views.pipeline_data_uploaded',
                slug=pipeline.slug,
                file_id=pipeline_data_file.id,
            )
        )

    if not file_contents.empty:
        file_contents = file_contents.to_dict()
    else:
        form.errors['non_field_errors'] = [
            'Unable to process file - please check your file is a valid CSV and try again'
        ]
    return render_uploader_template(
        'pipeline_data_verify.html',
        pipeline=pipeline,
        file_contents=file_contents,
        format_row_data=format_row_data,
        form=form,
    )


def format_row_data(row):
    return ", ".join(row.values())


@uploader_views.route('/data/<slug>/uploaded/<file_id>/')
@login_required
def pipeline_data_uploaded(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, id=file_id, deleted=False
    )
    schema_parts = pipeline.pipeline_schema.split('.')
    if len(schema_parts) != 2:
        raise BadRequest("Invalid schema for this pipeline.")
    data_workspace_schema_name = schema_parts[0]
    data_workspace_table_name = schema_parts[1]
    thread = process_pipeline_data_file(pipeline_data_file)
    file_id = pipeline_data_file.id
    thread.start()
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
    return '100' if file.processed_at else '0'
