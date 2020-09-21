from unittest.mock import ANY, call

from app.downloader.web.hmrc import HMRCExporters
from tests.downloader.mocks import inspect_online_mock, request_mock, storage_mock

html_string = b"""
    <html/>
        <a href="/Statistics/Documents/Data%20Downloads/exporters1703.zip">Exporters</a>
    </html>
"""


def test_hmrc_new_data_available(mocker):

    # Mocking
    download_file_mock = request_mock(mocker, {'*': b'some data'})
    s3_storage_mock = storage_mock(s3_keys_returned=['source/hmrc/exporters/exporters1702.zip'])
    get_data_files_html_mock = inspect_online_mock(mocker, html_string)

    # Run method to test
    exporters = HMRCExporters(s3_storage_mock)
    exporters.update()

    # Assert the correct html page is accessed twice to scan for new files
    get_data_files_html_mock.assert_has_calls([call(exporters.download_url, context=ANY)])

    # Assert correct files are downloaded
    download_file_mock.assert_has_calls(
        [
            call(
                'https://www.uktradeinfo.com/Statistics/Documents'
                '/Data%20Downloads/exporters1703.zip'
            )
        ],
        any_order=True,
    )

    # Assert correct files are written to s3 bucket
    s3_storage_mock.write_file.assert_has_calls(
        [call.write_file('exporters1703.zip', b'some data')]
    )


def test_hmrc_no_new_data(mocker):

    # Mocking
    download_file_mock = request_mock(mocker, {'*': b'some data'})
    s3_storage_mock = storage_mock(s3_keys_returned=['source/hmrc/exporters/exporters1703.zip'])
    get_data_files_html_mock = inspect_online_mock(mocker, html_string)

    # Run method to test
    exporters = HMRCExporters(s3_storage_mock)
    exporters.update()

    # Assert the correct html page is accessed to scan for new files
    get_data_files_html_mock.assert_called_with(exporters.download_url, context=ANY)

    # Assert no file is downloaded
    download_file_mock.assert_not_called()

    # Assert no new file is written to s3 bucket
    s3_storage_mock.write_file.assert_not_called()
