from tests.api.views import make_hawk_auth_request


BOUND_RATES_URL = '/api/v1/get-world-bank-bound-rates/?orientation=records'


def test_get_world_bank_bound_rates_empty_dataset(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, BOUND_RATES_URL)

    assert response.status_code == 200
    assert response.json == {'next': None, 'results': []}


def test_get_world_bank_bound_rates_single_row(
    app_with_hawk_user, app_with_mock_cache, add_world_bank_bound_rates
):
    add_world_bank_bound_rates([{'product': 201, 'reporter': 50, 'bound_rate': 14.0}])

    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, BOUND_RATES_URL)

    assert response.status_code == 200
    assert response.json == {
        'next': None,
        'results': [{'boundRate': 14.0, 'reporter': 50, 'product': 201}],
    }
