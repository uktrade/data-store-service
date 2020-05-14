from tests.api.views import make_hawk_auth_request


def test_get_table(app_with_hawk_user):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table/spire/batches')

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


def test_get_table_that_doesnt_exist(app_with_hawk_user):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/table/spire/batches_foo')

    assert response.status_code == 404
