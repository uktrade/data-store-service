from tests.api.views import make_hawk_auth_request


def test_get_reference_postcodes_when_no_data(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-reference-postcodes/')

    assert response.status_code == 200
    assert response.json['next'] is None
    assert response.json['values'] == []


def test_get_reference_postcodes(app_with_hawk_user, app_with_mock_cache, add_reference_postcodes):

    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
    ]

    add_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()

    expected_values = ['AB10 1AA', 'asdf']

    response = make_hawk_auth_request(client, '/api/v1/get-reference-postcodes/')

    assert response.status_code == 200
    assert response.json['next'] is None
    assert response.json['values'][0][:2] == expected_values


def test_get_reference_postcodes_next_url(
    app_with_hawk_user, app_with_mock_cache, add_reference_postcodes
):
    app_with_hawk_user.config['app']['pagination_size'] = 1
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]

    add_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()
    expected_values = ['AB10 1AA', 'asdf']

    response = make_hawk_auth_request(client, '/api/v1/get-reference-postcodes/')
    assert response.status_code == 200
    assert (
        response.json['next']
        == 'http://localhost/api/v1/get-reference-postcodes/?orientation=tabular&next-id=2'
    )
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == expected_values


def test_get_reference_postcodes_when_next_id_specified(
    app_with_hawk_user, app_with_mock_cache, add_reference_postcodes
):
    app_with_hawk_user.config['app']['pagination_size'] = 1
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]

    add_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()

    expected_values = ['ZZ10 1ZZ', 'zzzz']

    response = make_hawk_auth_request(client, '/api/v1/get-reference-postcodes/?next-id=2')
    assert response.status_code == 200
    assert response.json['next'] is None
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == expected_values
