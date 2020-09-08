from unittest.mock import ANY, call

from app.downloader.web.companies_house import CompaniesHouseAccounts
from tests.downloader.mocks import inspect_online_mock, request_mock, storage_mock

accounts_html_string = b"""
    <html/>
        <a href="Accounts_Bulk_Data-2017-06-20.zip">Accounts_Bulk_Data-2017-06-20.zip  (52Mb)</a>
        <a href="Accounts_Bulk_Data-2017-06-17.zip">Accounts_Bulk_Data-2017-06-17.zip  (37Mb)</a>
    </html>
"""


def test_ch_accounts_new_data_available(mocker):

    # Mocking
    download_file_mock = request_mock(mocker, {'*': b'some data'})
    s3_storage_mock = storage_mock(['Accounts_Bulk_Data-2017-06-17.zip'])
    get_data_files_html_mock = inspect_online_mock(mocker, accounts_html_string)

    # Run method to test
    datasource = CompaniesHouseAccounts(s3_storage_mock)
    datasource.update()

    # Assert the correct html page is accessed to scan for new files
    get_data_files_html_mock.assert_called_once_with(datasource.download_url, context=ANY)

    # Assert correct file is downloaded
    download_file_mock.assert_has_calls(
        [call('http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2017-06-20.zip')]
    )

    # Assert correct file is written to s3 bucket
    s3_storage_mock.write_file.assert_called_once_with(
        'Accounts_Bulk_Data-2017-06-20.zip', b'some data'
    )


def test_ch_accounts_no_new_data(mocker):

    # Mocking
    download_file_mock = request_mock(mocker, {'*': b'some data'})
    s3_storage_mock = storage_mock(['Accounts_Bulk_Data-2017-06-20.zip'])
    get_data_files_html_mock = inspect_online_mock(mocker, accounts_html_string)

    # Run method to test
    datasource = CompaniesHouseAccounts(s3_storage_mock)
    datasource.update()

    # Assert the correct html page is accessed to scan for new files
    get_data_files_html_mock.assert_called_once_with(datasource.download_url, context=ANY)

    # Assert no file is downloaded
    download_file_mock.assert_not_called()

    # Assert no new file is written to s3 bucket
    s3_storage_mock.write_file.assert_not_called()
