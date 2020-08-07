import datetime
import io
from unittest import mock

import pytest
from flask import template_rendered, url_for
from slugify import slugify

from app.constants import (
    DataUploaderFileState,
    NO,
    YES,
)
from app.db.models.internal import Pipeline, PipelineDataFile
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_select_pipeline_view_with_no_pipelines(
    is_authenticated, app_with_db, captured_templates
):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_select')
    response, template_context = _test_view(
        client, url, 'pipeline_select.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Add new dataset' in html
    assert template_context['show_form'] is False
    assert 'Continue to upload data' not in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_select_pipeline_view_with_pipelines(is_authenticated, app_with_db, captured_templates):
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_select_pipeline_view_radios_have_distinct_ids(
    is_authenticated, app_with_db, captured_templates
):
    PipelineFactory()
    PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_select')
    response, template_context = _test_view(
        client, url, 'pipeline_select.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert (
        '<input class="govuk-radios__input" id="pipeline-0" name="pipeline" type="radio" value="1">'
        in html
    )
    assert (
        '<input class="govuk-radios__input" id="pipeline-1" name="pipeline" type="radio" value="2">'
        in html
    )


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_submit_form_select_pipeline_view(is_authenticated, app_with_db, captured_templates):
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_pipeline_create_view(is_authenticated, app_with_db, captured_templates):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_create')
    response, template_context = _test_view(
        client, url, 'pipeline_create.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Create pipeline' in html
    assert 'Add new dataset' not in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_submit_form_pipeline_create_view(is_authenticated, app_with_db, captured_templates):
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@pytest.mark.parametrize(
    "organisation, dataset, expected_error",
    (
        ("schema", "table", False),
        ("schema.valid", "table123", False),
        ("schema", "table_name", False),
        ("schema", " strips_spaces_ok ", False),
        ("schema-invalid", "table", True),
        ("schema(bad)", "table@thing", True),
        ("schema spaces", "table&now", True),
    ),
)
def test_submit_form_with_bad_names_on_pipeline_create_view(
    is_authenticated, app_with_db, captured_templates, organisation, dataset, expected_error
):
    form_data = {'organisation': organisation, 'dataset': dataset}

    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_create')
    post_response = client.post(url, data=form_data, follow_redirects=True)

    assert post_response.status_code == 200

    if expected_error:
        assert (
            "Please choose a name that meets our dataset naming conventions."
            in post_response.get_data(as_text=True)
        )

    else:
        assert "Dataset pipeline created" in post_response.get_data(as_text=True)


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
@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_submit_form_pipeline_create_view_errors(
    is_authenticated, form_data, expected_error, app_with_db, captured_templates
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_pipeline_created_view(is_authenticated, app_with_db, captured_templates):
    pipeline = PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_created', slug=pipeline.slug)
    response, template_context = _test_view(
        client, url, 'pipeline_created.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Dataset pipeline created' in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_pipeline_created_view_404_unknown_pipeline(
    is_authenticated, app_with_db, captured_templates
):
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_created', slug='i-made-this-up')
    response = make_sso_request(client, url)
    assert response.status_code == 404


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.views.process_pipeline_data_file')
def test_get_data_uploaded_view(
    is_authenticated, process_pipeline_data_file, app_with_db, captured_templates
):
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


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_data_upload_view(is_authenticated, app_with_db, captured_templates):
    pipeline = PipelineFactory()
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)
    response, template_context = _test_view(
        client, url, 'pipeline_data_upload.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Upload data' in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
def test_get_data_verify_view(mock_smart_open, is_authenticated, app_with_db, captured_templates):
    csv_string = 'hello,goodbye\n1,2\n3,4'
    mock_smart_open.return_value = io.StringIO(csv_string)
    data_file = PipelineDataFileFactory(pipeline__delimiter=',')
    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_data_verify', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    response, template_context = _test_view(
        client, url, 'pipeline_data_verify.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
def test_get_data_verify_error_view(
    mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    mock_smart_open.side_effect = UnicodeDecodeError('Error', b'', 1, 2, '')

    data_file = PipelineDataFileFactory()
    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_data_verify', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    response, template_context = _test_view(
        client, url, 'pipeline_data_verify.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Try again' in html

    form = template_context['form']
    assert form.errors == {
        'non_field_errors': [
            "Unable to process file - 'Error' codec can't decode bytes in position 1-1: "
        ]
    }


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.views.delete_file')
def test_submit_data_verify_proceed_no(
    mock_delete_file, is_authenticated, app_with_db, captured_templates
):
    data_file = PipelineDataFileFactory()
    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_data_verify', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    form_data = {'proceed': NO}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    html = response.get_data(as_text=True)
    assert 'Continue to upload data' in html
    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_select.html'
    assert template_context['request'].path == url_for('uploader_views.pipeline_select')
    assert not PipelineDataFile.query.get(data_file.id)
    assert mock_delete_file.called is True


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.views.process_pipeline_data_file')
@mock.patch('app.uploader.utils.open')
def test_submit_data_verify_proceed_yes(
    mock_smart_open,
    mock_process_pipeline_data_file,
    is_authenticated,
    app_with_db,
    captured_templates,
):
    mock_thread = mock.Mock()
    mock_process_pipeline_data_file.return_value = mock_thread
    csv_string = 'hello,goodbye\n1,2\n3,4'
    mock_smart_open.return_value = io.StringIO(csv_string)
    data_file = PipelineDataFileFactory(pipeline__delimiter=',')
    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_data_verify', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    form_data = {'proceed': YES}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    html = response.get_data(as_text=True)
    assert 'Data now being processed' in html
    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_data_uploaded.html'
    assert template_context['request'].path == url_for(
        'uploader_views.pipeline_data_uploaded', slug=data_file.pipeline.slug, file_id=data_file.id
    )
    assert PipelineDataFile.query.get(data_file.id)
    assert mock_process_pipeline_data_file.called is True
    assert mock_thread.start.called is True


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
@mock.patch('app.uploader.views.upload_file')
def test_submit_data_upload_view(
    mock_upload_file, mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    csv_string = 'hello,goodbye\n1,2\n3,4'
    mock_smart_open.return_value = io.StringIO(csv_string)
    pipeline = PipelineFactory(delimiter=",", quote='"')
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)
    form_data = {'csv_file': (io.BytesIO(bytes(csv_string, encoding='utf-8')), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html

    template, template_context = captured_templates.pop()
    file_contents = template_context['file_contents']
    assert file_contents == {'goodbye': {0: '2', 1: '4'}, 'hello': {0: '1', 1: '3'}}

    assert pipeline.data_files[0].data_file_url.split("/")[1] == pipeline.organisation
    assert pipeline.data_files[0].data_file_url.split("/")[2] == pipeline.dataset


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
@mock.patch('app.uploader.views.upload_file')
def test_submit_data_upload_view_additional_dataset(
    mock_upload_file, mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    file_1_csv_string = 'hello,goodbye\n1,2\n3,4'
    file_2_csv_string = 'goodbye,hello\n1,2\n3,4'

    mock_smart_open.return_value = io.StringIO(file_1_csv_string)
    mock_upload_file.return_value = 'fakefile_1.csv'
    pipeline = PipelineFactory(delimiter=",", quote='"')
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)

    # upload first file
    form_data = {'csv_file': (io.BytesIO(bytes(file_1_csv_string, encoding='utf-8')), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html

    template, template_context = captured_templates.pop()
    file_contents = template_context['file_contents']
    assert file_contents == {'goodbye': {0: '2', 1: '4'}, 'hello': {0: '1', 1: '3'}}
    assert len(pipeline.data_files) == 1
    assert pipeline.data_files[0].data_file_url.split("/")[1] == pipeline.organisation
    assert pipeline.data_files[0].data_file_url.split("/")[2] == pipeline.dataset

    # upload another file
    mock_smart_open.side_effect = [io.StringIO(file_2_csv_string), io.StringIO(file_1_csv_string)]
    mock_upload_file.return_value = 'fakefile_2.csv'

    form_data = {'csv_file': (io.BytesIO(bytes(file_2_csv_string, encoding='utf-8')), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html

    template, template_context = captured_templates.pop()
    file_contents = template_context['file_contents']
    assert file_contents == {'goodbye': {0: '1', 1: '3'}, 'hello': {0: '2', 1: '4'}}
    assert len(pipeline.data_files) == 2
    assert pipeline.data_files[0].data_file_url.split("/")[1] == pipeline.organisation
    assert pipeline.data_files[0].data_file_url.split("/")[2] == pipeline.dataset
    assert pipeline.data_files[1].data_file_url.split("/")[1] == pipeline.organisation
    assert pipeline.data_files[1].data_file_url.split("/")[2] == pipeline.dataset


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
@mock.patch('app.uploader.views.upload_file')
def test_submit_data_upload_view_where_new_file_is_missing_columns_present_in_previous_file(
    mock_upload_file, mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    file_1_csv_string = 'one,two\n1,2\n3,4'
    file_2_csv_string = 'two,three\n1,2\n3,4'

    mock_smart_open.return_value = io.StringIO(file_1_csv_string)
    mock_upload_file.return_value = 'fakefile_1.csv'
    pipeline = PipelineFactory(delimiter=",", quote='"')
    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)

    # upload first file
    form_data = {'csv_file': (io.BytesIO(bytes(file_1_csv_string, encoding='utf-8')), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html

    # upload another file
    mock_smart_open.side_effect = [io.StringIO(file_2_csv_string), io.StringIO(file_1_csv_string)]
    mock_upload_file.return_value = 'fakefile_2.csv'

    form_data = {'csv_file': (io.BytesIO(bytes(file_2_csv_string, encoding='utf-8')), 'test.csv')}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    assert mock_upload_file.called is True
    html = response.get_data(as_text=True)
    assert 'Data successfully uploaded' in html
    assert 'Missing column: one' in html
    assert 'Continue with missing columns' in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
def test_get_data_upload_view_existing_versions(is_authenticated, app_with_db, captured_templates):
    pipeline = PipelineFactory()
    # Latest version
    PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 7, 1),
    )
    # Version that can be restored
    PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 6, 1),
    )
    # Version that is yet to be processed
    PipelineDataFileFactory(
        pipeline=pipeline, state=DataUploaderFileState.UPLOADED.value, processed_at=None
    )

    client = get_client(app_with_db)
    url = url_for('uploader_views.pipeline_data_upload', slug=pipeline.slug)
    response, template_context = _test_view(
        client, url, 'pipeline_data_upload.html', captured_templates,
    )
    html = response.get_data(as_text=True)

    assert 'Latest' in html  # data_file_1
    assert 'Restore' in html  # data_file_2
    assert 'Process' in html  # data_file_3


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
def test_get_restore_version_view(
    mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    csv_string_1 = 'hello,goodbye\n1,2\n3,4'  # latest
    csv_string_2 = 'hi,bye\n1,2\n3,4'  # version to restore
    # The view first gets the s3 sample of the latest version and then the version to restore
    mock_smart_open.side_effect = [io.StringIO(csv_string_1), io.StringIO(csv_string_2)]

    pipeline = PipelineFactory(delimiter=',')
    # Latest version
    PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 7, 1),
    )
    # Version that can be restored
    data_file_2 = PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 6, 1),
    )

    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_restore_version', slug=pipeline.slug, file_id=data_file_2.id
    )
    response, template_context = _test_view(
        client, url, 'pipeline_restore_version.html', captured_templates,
    )
    html = response.get_data(as_text=True)
    assert 'Latest version' in html  # data_file_1
    assert 'hello' in html
    assert 'goodbye' in html
    assert 'Version to restore' in html  # data_file_2
    assert 'hi' in html
    assert 'bye' in html


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.utils.open')
def test_submit_restore_version_cancel(
    mock_smart_open, is_authenticated, app_with_db, captured_templates
):
    pipeline = PipelineFactory()
    # Latest version
    PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 7, 1),
    )
    # Version that can be restored
    data_file_2 = PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 6, 1),
    )

    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_restore_version', slug=pipeline.slug, file_id=data_file_2.id
    )
    form_data = {'proceed': NO}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )
    html = response.get_data(as_text=True)
    assert 'Existing versions' in html
    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_data_upload.html'
    assert template_context['request'].path == url_for(
        'uploader_views.pipeline_data_upload', slug=pipeline.slug
    )


@mock.patch('data_engineering.common.sso.token.is_authenticated', return_value=True)
@mock.patch('app.uploader.views.process_pipeline_data_file')
@mock.patch('app.uploader.utils.open')
def test_submit_restore_version_proceed(
    mock_smart_open,
    mock_process_pipeline_data_file,
    is_authenticated,
    app_with_db,
    captured_templates,
):
    mock_thread = mock.Mock()
    mock_process_pipeline_data_file.return_value = mock_thread
    csv_string = 'hello,goodbye\n1,2\n3,4'
    # The view first gets the s3 sample of the latest version and then the version to restore
    mock_smart_open.side_effect = [io.StringIO(csv_string), io.StringIO(csv_string)]

    pipeline = PipelineFactory(delimiter=',')
    # Latest version
    PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 7, 1),
    )
    # Version that can be restored
    data_file_2 = PipelineDataFileFactory(
        pipeline=pipeline,
        state=DataUploaderFileState.COMPLETED.value,
        processed_at=datetime.datetime(2020, 6, 1),
    )

    client = get_client(app_with_db)
    url = url_for(
        'uploader_views.pipeline_restore_version', slug=pipeline.slug, file_id=data_file_2.id
    )
    form_data = {'proceed': YES}
    response = client.post(
        url, data=form_data, follow_redirects=True, content_type='multipart/form-data'
    )

    html = response.get_data(as_text=True)
    assert 'Data now being processed' in html
    template, template_context = captured_templates.pop()
    assert template.name == 'pipeline_data_uploaded.html'
    assert template_context['request'].path == url_for(
        'uploader_views.pipeline_data_uploaded', slug=pipeline.slug, file_id=data_file_2.id
    )
    assert mock_process_pipeline_data_file.called is True
    assert mock_thread.start.called is True
