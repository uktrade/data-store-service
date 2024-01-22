from datetime import datetime

from app.downloader.web.ons import ONSPostcodeDirectory
from tests.downloader.mocks import inspect_online_selenium_mock, request_mock, storage_mock

download_url_response_dict = {
    (
        'https://www.arcgis.com/sharing/rest/content/items/a644dd04d18f4592b7d36705f93270d8/data'
    ): b'some data',
    (
        'https://www.arcgis.com/sharing/rest/content/items/a644dd04d18f4592b7d36705f9327dse/data'
    ): b'some data',
}

selenium_url_response_dict = {
    'https://geoportal.statistics.gov.uk/search?sort=-created&tags=ons%20postcode%20directory': [
        (
            'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-august-2020',
            datetime.strptime('01082020', "%d%m%Y"),
        ),
        (
            'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-may-2020',
            datetime.strptime('01052020', "%d%m%Y"),
        ),
    ],
    'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-august-2020': [
        (
            (
                'https://www.arcgis.com/sharing/rest/'
                'content/items/a644dd04d18f4592b7d36705f93270d8/data'
            ),
            None,
        )
    ],
    'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-may-2020': [
        (
            (
                'https://www.arcgis.com/sharing/rest/'
                'content/items/a644dd04d18f4592b7d36705f9327dse/data'
            ),
            None,
        )
    ],
}


def test_ons_postcode_directory_new_data_available(mocker):
    # Mocking
    download_file_mock = request_mock(mocker, download_url_response_dict)
    get_data_files_html_mock = inspect_online_selenium_mock(mocker, selenium_url_response_dict)
    s3_storage_mock = storage_mock(['ONSPD_MAY_2020_UK.zip'])

    # Run method to test
    datasource = ONSPostcodeDirectory(s3_storage_mock)
    datasource.update()

    # Assert the correct html page is accessed to scan for new files
    assert get_data_files_html_mock.inspected_urls == [
        'https://geoportal.statistics.gov.uk/search?sort=-created&tags=ons%20postcode%20directory',
        'https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-august-2020',
    ]

    # Assert correct file is downloaded
    download_file_mock.assert_called_once_with(
        'https://www.arcgis.com/sharing/rest/content/items/a644dd04d18f4592b7d36705f93270d8/data'
    )

    # Assert correct file is written to s3 bucket
    s3_storage_mock.write_file.assert_called_once_with('ONSPD_AUG_2020_UK.zip', b'some data')


def test_ons_postcode_no_new_data(mocker):
    # Mocking
    download_file_mock = request_mock(mocker, download_url_response_dict)
    get_data_files_html_mock = inspect_online_selenium_mock(mocker, selenium_url_response_dict)
    s3_storage_mock = storage_mock(['ONSPD_AUG_2020_UK.zip'])

    # Run method to test
    datasource = ONSPostcodeDirectory(s3_storage_mock)
    datasource.update()

    # Assert the correct html page is accessed to scan for new files
    assert get_data_files_html_mock.inspected_urls == [
        'https://geoportal.statistics.gov.uk/search?sort=-created&tags=ons%20postcode%20directory',
    ]

    # Assert no file is downloaded
    download_file_mock.assert_not_called()

    # Assert no new file is written to s3 bucket
    s3_storage_mock.write_file.assert_not_called()
