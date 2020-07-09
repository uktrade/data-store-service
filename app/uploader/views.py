import os
import uuid

from data_engineering.common.sso.token import login_required
from flask import abort, redirect, render_template, url_for
from flask.blueprints import Blueprint

from app.constants import YES
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
            dataset=form.dataset.data,
            organisation=form.organisation.data,
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
        data_file_url = upload_file(form.csv_file.data, pipeline, uuid.uuid4())
        data_file = PipelineDataFile(
            data_file_url=data_file_url, pipeline=pipeline, latest_version=False
        )
        data_file.save()
        return redirect(
            url_for('uploader_views.pipeline_data_verify', slug=pipeline.slug, file_id=data_file.id)
        )

    return render_uploader_template(
        'pipeline_data_upload.html', pipeline=pipeline, form=form, heading='Upload data'
    )


@uploader_views.route('/data/<slug>/verify/<file_id>/', methods=('GET', 'POST'))
@login_required
def pipeline_data_verify(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    pipeline_data_file = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, id=file_id
    )

    form = forms.VerifyDataFileForm()
    is_form_valid = form.validate_on_submit()
    if is_form_valid and form.proceed.data != YES:
        pipeline_data_file.delete()
        delete_file(pipeline_data_file)
        return redirect(url_for('uploader_views.pipeline_select'))

    file_contents = get_s3_file_sample(
        pipeline_data_file.data_file_url, pipeline.delimiter, pipeline.quote
    )
    if is_form_valid:
        pipeline.column_types = get_column_types(file_contents)
        pipeline.save()

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


@uploader_views.route('/data/<slug>/restore/<file_id>/', methods=('GET', 'POST'))
def pipeline_restore_version(slug, file_id):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    form = forms.RestoreVersionForm()
    is_form_valid = form.validate_on_submit()
    if is_form_valid and form.proceed.data != YES:
        return redirect(url_for(
            'uploader_views.pipeline_data_upload',
            slug=pipeline.slug,
        ))

    data_file_latest = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, latest_version=True
    )
    file_contents_latest = get_s3_file_sample(
        data_file_latest.data_file_url, pipeline.delimiter, pipeline.quote
    )

    data_file_to_restore = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, id=file_id
    )
    file_contents_to_restore = get_s3_file_sample(
        data_file_to_restore.data_file_url, pipeline.delimiter, pipeline.quote
    )

    if is_form_valid:
        pipeline.column_types = get_column_types(file_contents_to_restore)
        pipeline.save()

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
    pipeline_data_file = get_object_or_404(
        PipelineDataFile, pipeline=pipeline, id=file_id
    )
    thread = process_pipeline_data_file(pipeline_data_file)
    file_id = pipeline_data_file.id
    thread.start()
    return render_uploader_template(
        'pipeline_data_uploaded.html', pipeline=pipeline, file_id=file_id
    )


@uploader_views.route('/progress/<file_id>/')
@login_required
def progress(file_id):
    file = get_object_or_404(PipelineDataFile, id=file_id)
    if file.processed_at:
        # Set existing latest_version to False
        for data_file in file.pipeline.data_files:
            if data_file.latest_version and data_file.id != file.id:
                data_file.latest_version = False
                data_file.save()
        return '100'
    return '0'
