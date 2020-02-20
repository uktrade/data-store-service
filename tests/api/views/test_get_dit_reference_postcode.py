from tests.api.views import make_hawk_auth_request


def test_alias_postcode_route(
    add_dit_reference_postcodes, app_with_hawk_user, app_with_mock_cache
):
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]
    url = '/api/v1/get-postcode-data/?postcode=AB10%201AA'
    add_dit_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['AB10 1AA', 'asdf']


def test_get_dit_reference_postcode_when_no_data(app_with_hawk_user, app_with_mock_cache):
    url = '/api/v1/get-dit-reference-postcode/?postcode=AB10%201AA'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert response.json['values'] == []


def test_get_dit_reference_postcode(
    add_dit_reference_postcodes, app_with_hawk_user, app_with_mock_cache
):
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]
    url = '/api/v1/get-dit-reference-postcode/?postcode=AB10%201AA'
    add_dit_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['AB10 1AA', 'asdf']

    url = '/api/v1/get-dit-reference-postcode/?postcode=ZZ10%201ZZ'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['ZZ10 1ZZ', 'zzzz']


def test_lower_case_and_whitespace_dit_reference_postcode(
    add_dit_reference_postcodes, app_with_hawk_user, app_with_mock_cache
):
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]
    url = '/api/v1/get-dit-reference-postcode/?postcode=AB101AA'
    add_dit_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['AB10 1AA', 'asdf']

    url = '/api/v1/get-dit-reference-postcode/?postcode=zz10%201zz'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['ZZ10 1ZZ', 'zzzz']

    url = '/api/v1/get-dit-reference-postcode/?postcode=zz10%201zz%20%20'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['ZZ10 1ZZ', 'zzzz']
