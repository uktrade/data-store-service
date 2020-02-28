from tests.api.views import make_hawk_auth_request


def test_get_world_bank_tariffs_empty_dataset(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-world-bank-tariffs/')

    assert response.status_code == 200
    assert response.json == {'next': None, 'results': []}
