from tests.api.views import make_hawk_auth_request


def test_get_world_bank_tariffs_empty_dataset(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-world-bank-tariffs/?orientation=records')

    assert response.status_code == 200
    assert response.json == {'next': None, 'results': []}


def test_get_world_bank_tariffs_single_row(
    app_with_hawk_user, app_with_mock_cache, add_world_bank_tariff
):
    add_world_bank_tariff(
        [
            {
                'data_source_row_id': 1,
                'product': 201,
                'reporter': 50,
                'partner': 52,
                'year': 1990,
                'assumed_tariff': 10.0,
                'app_rate': 11.0,
                'mfn_rate': 12.0,
                'prf_rate': 13.0,
                'bnd_rate': 14.0,
                'country_average': 15.0,
                'world_average': 16.0,
            }
        ]
    )

    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-world-bank-tariffs/?orientation=records')

    assert response.status_code == 200
    assert response.json == {
        'next': None,
        'results': [
            {
                'appRate': 11.0,
                'assumedTariff': 10.0,
                'bndRate': 14.0,
                'countryAverage': 15.0,
                'mfnRate': 12.0,
                'partner': 52,
                'prfRate': 13.0,
                'product': 201,
                'reporter': 50,
                'worldAverage': 16.0,
                'year': 1990,
            }
        ],
    }
