from tests.api.views import make_hawk_auth_request

ONS_POSTCODE_FIELDS = [
    'pcd',
    'pcd2',
    'pcds',
    'dointr',
    'doterm',
    'oscty',
    'ced',
    'oslaua',
    'osward',
    'parish',
    'usertype',
    'oseast1m',
    'osnrth1m',
    'osgrdind',
    'oshlthau',
    'nhser',
    'ctry',
    'rgn',
    'streg',
    'pcon',
    'eer',
    'teclec',
    'ttwa',
    'pct',
    'nuts',
    'statsward',
    'oa01',
    'casward',
    'park',
    'lsoa01',
    'msoa01',
    'ur01ind',
    'oac01',
    'oa11',
    'lsoa11',
    'msoa11',
    'wz11',
    'ccg',
    'bua11',
    'buasd11',
    'ru11ind',
    'oac11',
    'lat',
    'long',
    'lep1',
    'lep2',
    'pfa',
    'imd',
    'calncv',
    'stp',
]

def test_get_ons_postcodes_when_no_data(app_with_hawk_user, app_with_mock_cache):
    client = app_with_hawk_user.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-ons-postcodes/')

    assert response.status_code == 200
    assert response.json == {'headers': ONS_POSTCODE_FIELDS, 'next': None, 'values': []}


def test_get_ons_postcodes(app_with_hawk_user, app_with_mock_cache, add_ons_postcode):

    postcode = 'AB1 1BA'

    add_ons_postcode([{'postcode': postcode}])
    client = app_with_hawk_user.test_client()

    expected_result = [None] * len(ONS_POSTCODE_FIELDS)
    expected_result[2] = postcode

    response = make_hawk_auth_request(client, '/api/v1/get-ons-postcodes/')

    assert response.status_code == 200
    assert response.json == {
        'headers': ONS_POSTCODE_FIELDS,
        'next': None,
        'values': [expected_result],
    }


def test_get_ons_postcodes_next_url(app_with_hawk_user, app_with_mock_cache, add_ons_postcode):
    app_with_hawk_user.config['app']['pagination_size'] = 1
    postcode_1 = 'AB1 1BA'
    postcode_2 = 'AB2 2BA'

    add_ons_postcode([{'postcode': postcode_1}, {'postcode': postcode_2}])
    client = app_with_hawk_user.test_client()

    expected_result = [None] * len(ONS_POSTCODE_FIELDS)
    expected_result[2] = postcode_1

    response = make_hawk_auth_request(client, '/api/v1/get-ons-postcodes/')
    assert response.status_code == 200
    assert response.json == {
        'headers': ONS_POSTCODE_FIELDS,
        'next': 'http://localhost/api/v1/get-ons-postcodes/?orientation=tabular&next-id=2',
        'values': [expected_result],
    }


def test_get_ons_postcodes_when_next_id_specified(
    app_with_hawk_user, app_with_mock_cache, add_ons_postcode
):
    app_with_hawk_user.config['app']['pagination_size'] = 1
    postcode_1 = 'AB1 1BA'
    postcode_2 = 'AB2 2BA'

    add_ons_postcode([{'postcode': postcode_1}, {'postcode': postcode_2}])
    client = app_with_hawk_user.test_client()

    expected_result = [None] * len(ONS_POSTCODE_FIELDS)
    expected_result[2] = postcode_2

    response = make_hawk_auth_request(client, '/api/v1/get-ons-postcodes/?next-id=2')
    assert response.status_code == 200
    assert response.json == {
        'headers': ONS_POSTCODE_FIELDS,
        'next': None,
        'values': [expected_result],
    }
