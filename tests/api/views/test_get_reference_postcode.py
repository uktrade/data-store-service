import pytest
from tests.api.views import make_hawk_auth_request    


def test_get_reference_postcodes_when_no_data(
        app_with_hawk_user,
        app_with_mock_cache
):
    url = '/api/v1/get-reference-postcode/?reference-postcode=AB10%201AA'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert response.json['values'] == []


def test_get_reference_postcodes(
        add_reference_postcodes,
        app_with_hawk_user,
        app_with_mock_cache
):
    postcodes = [
        {'postcode': 'AB10 1AA', 'local_authority_district_code': 'asdf'},
        {'postcode': 'ZZ10 1ZZ', 'local_authority_district_code': 'zzzz'},
    ]
    url = '/api/v1/get-reference-postcode/?reference-postcode=AB10%201AA'
    add_reference_postcodes(postcodes)
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['AB10 1AA', 'asdf']

    url = '/api/v1/get-reference-postcode/?reference-postcode=ZZ10%201ZZ'
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, url)
    assert response.status_code == 200
    assert len(response.json['values']) == 1
    assert response.json['values'][0][:2] == ['ZZ10 1ZZ', 'zzzz']
    
