from tests.api.views import make_hawk_auth_request
from tests.fixtures.factories import SPIREFootnoteFactory


def test_get_table_structure(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table-structure/spire/batches')
    assert response.status_code == 200
    assert response.json == {
        'columns': [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'batch_ref', 'type': 'TEXT'},
            {'name': 'status', 'type': 'TEXT'},
            {'name': 'start_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
            {'name': 'end_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
            {'name': 'approve_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
            {'name': 'release_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
            {'name': 'staging_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
        ],
    }


def test_get_table_structure_that_doesnt_exist(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table-structure/spire/batches_foo')
    assert response.status_code == 404


def test_get_table_structure_in_excluded_schema(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table-structure/public/hawk_users')
    assert response.status_code == 404


def test_get_table_data(app_with_hawk_user, app_with_mock_cache):
    SPIREFootnoteFactory(text='foo bar', status='CURRENT')
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(
        client, '/api/v1/table-data/spire/footnotes?orientation=records'
    )
    assert response.status_code == 200
    assert response.json == {'results': [{'text': 'foo bar', 'status': 'CURRENT'}], 'next': None}


def test_get_table_data_paginated(app_with_hawk_user, app_with_mock_cache):
    app_with_hawk_user.config['app']['pagination_size'] = 10

    for i in range(15):
        SPIREFootnoteFactory(text='foo bar', status='CURRENT')

    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(
        client, '/api/v1/table-data/spire/footnotes?orientation=records'
    )
    expected_next_url = (
        'http://localhost/api/v1/table-data/spire/footnotes?orientation=records&next-id=11'
    )

    assert response.status_code == 200
    assert response.json == {
        'results': [{'text': 'foo bar', 'status': 'CURRENT'} for i in range(10)],
        'next': expected_next_url,
    }

    # Go to next page
    response = make_hawk_auth_request(client, response.json['next'].replace('http://localhost', ''))
    assert response.status_code == 200
    assert response.json == {
        'results': [{'text': 'foo bar', 'status': 'CURRENT'} for i in range(10, 15)],
        'next': None,
    }


def test_get_table_data_that_doesnt_exist(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table-data/spire/batches_foo')
    assert response.status_code == 404


def test_get_table_data_that_doesnt_have_id_columns(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table-data/spire/applications')
    assert response.status_code == 400
