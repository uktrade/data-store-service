import os

from flask import abort, redirect, render_template, url_for
from flask.blueprints import Blueprint

from app.db.models.internal import Pipeline
from app.uploader.forms import DataFileForm, PipelineForm, PipelineSelectForm
from app.uploader.utils import upload_file

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
def pipeline_create():
    form = PipelineForm()
    if form.validate_on_submit():
        pipeline = Pipeline(dataset=form.dataset.data, organisation=form.organisation.data)
        pipeline.save()
        return redirect(url_for('uploader_views.pipeline_created', slug=pipeline.slug))
    return render_uploader_template('pipeline_create.html', heading='Create dataset', form=form)


@uploader_views.route('/created/<slug>/')
def pipeline_created(slug):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    return render_uploader_template('pipeline_created.html', pipeline=pipeline)


@uploader_views.route('/data/<slug>/', methods=('GET', 'POST'))
def pipeline_data_upload(slug):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    form = DataFileForm()
    if form.validate_on_submit():
        upload_file(form.csv_file.data, form.csv_file.data.filename, pipeline)
        return redirect(url_for('uploader_views.pipeline_data_uploaded', slug=pipeline.slug))
    return render_uploader_template(
        'pipeline_data_upload.html', pipeline=pipeline, form=form, heading='Upload data'
    )


@uploader_views.route('/data/<slug>/uploaded/')
def pipeline_data_uploaded(slug):
    pipeline = get_object_or_404(Pipeline, slug=slug)
    return render_uploader_template('pipeline_data_uploaded.html', pipeline=pipeline)
