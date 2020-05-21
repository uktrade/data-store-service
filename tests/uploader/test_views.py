import io
from unittest import mock

import pytest
from flask import template_rendered, url_for
from slugify import slugify

from app.constants import DEFAULT_CSV_DELIMITER, DEFAULT_CSV_QUOTECHAR
from app.db.models.internal import Pipeline
from tests.api.views import make_sso_request
from tests.fixtures.factories import PipelineDataFileFactory, PipelineFactory


def get_client(app):
    app.config['WTF_CSRF_ENABLED'] = False
    request_context = app.test_request_context()
    request_context.push()
    return app.test_client()


@pytest.fixture
def captured_templates(app):
    recorded = []

    def record(sender, template, context, **extra):
        recorded.append((template, context))

    template_rendered.connect(record, app)
    try:
        yield recorded
    finally:
        template_rendered.disconnect(record, app)


def _test_view(client, url, expected_template_name, _templates, expected_status=200):
    response = make_sso_request(client, url)
    assert response.status_code == expected_status
    assert len(_templates) == 1
    template, template_context = _templates.pop()
    assert template.name == expected_template_name
    assert template_context['request'].path == url
    return response, template_context


def test_get_select_pipeline_view_with_no_pipelines(app_with_db, captured_templates):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_select')
    response, template_context = _test_view(
        client, url, 'pipeline_select.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Add new dataset' in html
    assert template_context['show_form'] is False
    assert 'Continue to upload data' not in html


def test_get_select_pipeline_view_with_pipelines(app_with_db, captured_templates):
    PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_select')
    response, template_context = _test_view(
        client, url, 'pipeline_select.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Add new dataset' in html
    assert template_context['show_form'] is True
    assert 'Continue to upload data' in html


def test_submit_form_select_pipeline_view(app_with_db, captured_templates):
    pipeline = PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_select')
    response = client.post(url, data={'pipeline': pipeline.id}, follow_redirects=True)

    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_data_upload.html'
    assert template_context['request'].path == url_for(
        'uploader_views.pipeline_data_upload', slug=pipeline.slug
    )
    html = response.get_data(as_text=True)
    assert 'Upload data' in html


def test_get_pipeline_create_view(app_with_db, captured_templates):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_create')
    response, template_context = _test_view(
        client, url, 'pipeline_create.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Create pipeline' in html
    assert 'Add new dataset' not in html


def test_submit_form_pipeline_create_view(app_with_db, captured_templates):
    form_data = {'organisation': 'test_org', 'dataset': 'test_dataset'}
    slug = slugify(f'{form_data["organisation"]} {form_data["dataset"]}')
    assert Pipeline.query.filter_by(slug=slug).first() is None

    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_create')
    post_response = client.post(url, data=form_data, follow_redirects=True)
    html = post_response.get_data(as_text=True)
    assert 'Dataset pipeline created' in html

    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_created.html'
    assert template_context['request'].path == url_for('uploader_views.pipeline_created', slug=slug)

    pipeline = Pipeline.query.filter_by(slug=slug).first()
    assert pipeline is not None
    assert pipeline.organisation == form_data['organisation']
    assert pipeline.dataset == form_data['dataset']


@pytest.mark.parametrize(
    'form_data,expected_error',
    (
        ({}, {'dataset': ['This field is required.'], 'organisation': ['This field is required.']}),
        ({'organisation': 'test_org_1'}, {'dataset': ['This field is required.']}),
        ({'dataset': 'test_dataset_1'}, {'organisation': ['This field is required.']}),
        (
            {'organisation': 'test_organisation', 'dataset': 'test_dataset'},
            {'non_field_errors': ['Pipeline for organisation and dataset already exists']},
        ),
    ),
)
def test_submit_form_pipeline_create_view_errors(
    form_data, expected_error, app_with_db, captured_templates
):
    PipelineFactory(dataset='test_dataset', organisation='test_organisation')
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_create')
    post_response = client.post(url, data=form_data, follow_redirects=True)
    html = post_response.get_data(as_text=True)
    assert 'Dataset pipeline created' not in html
    template, template_context = captured_templates.pop()
    form = template_context['form']
    assert form.errors == expected_error


def test_get_pipeline_created_view(app_with_db, captured_templates):
    pipeline = PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_created', slug=pipeline.slug)
    response, template_context = _test_view(
        client, url, 'pipeline_created.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Dataset pipeline created' in html


def test_get_pipeline_created_view_404_unknown_pipeline(app_with_db, captured_templates):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_created', slug='i-made-this-up')
    response = make_sso_request(client, url)
    assert response.status_code == 404


def test_get_data_uploaded_view(app_with_db, captured_templates):
    data_file = PipelineDataFileFactory()
    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_data_uploaded', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    response, template_context = _test_view(
        client, url, 'pipeline_data_uploaded.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Data now being processed' in html


def test_get_data_upload_view(app_with_db, captured_templates):
    pipeline = PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)
    response, template_context = _test_view(
        client, url, 'pipeline_data_upload.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Upload data' in html


@mock.patch('app.uploader.utils.open')
@mock.patch('app.uploader.views.upload_file')
def test_submit_data_upload_view(
    mock_upload_file, mock_smart_open, app_with_db, captured_templates
):
    csv_string = 'hello,goodbye\n1,2\n3,4'
    mock_smart_open.return_value = io.StringIO(csv_string)
    mock_upload_file.return_value = 'fakefile.csv'
    pipeline = PipelineFactory(delimiter=DEFAULT_CSV_DELIMITER, quote=DEFAULT_CSV_QUOTECHAR)
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)
    form_data = {'csv_file': (io.BytesIO(b"hello,goodbye\n1,2\n3,4"), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html

    template, template_context = captured_templates.pop()
    file_contents = template_context['file_contents']
    assert file_contents == {'goodbye': {0: '2', 1: '4'}, 'hello': {0: '1', 1: '3'}}
