from tests.api.views import make_hawk_auth_request


BACI_DATA_URL = '/api/v1/get-dit-baci-data/?orientation=records'


def test_get_dit_baci_data_empty_dataset(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, BACI_DATA_URL)

    assert response.status_code == 200
    assert response.json == {'next': None, 'results': []}


def test_get_dit_baci_data_single_row(
    app_with_hawk_user, app_with_mock_cache, add_dit_baci
):
    add_dit_baci(
        [
            {
                'year': 1995,
                'product_category': 10519,
                'exporter': 4,
                'importer': 251,
                'trade_flow_value': 1.548,
                'quantity': 0.051,
            }
        ]
    )

    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, BACI_DATA_URL)

    assert response.status_code == 200
    assert response.json == {
        'next': None,
        'results': [
            {
                'year': 1995,
                'productCategory': 10519,
                'exporter': 4,
                'importer': 251,
                'tradeFlowValue': 1.548,
                'quantity': 0.051,
            },
        ],
    }
