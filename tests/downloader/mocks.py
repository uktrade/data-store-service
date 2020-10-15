import urllib
from io import BytesIO
from unittest.mock import MagicMock

import requests

from app.downloader.inspector import DataUrl, SeleniumOnlineInspector


def storage_mock(s3_keys_returned=None):
    # patch s3 storage
    s3_storage = MagicMock(autospec=True)

    if s3_keys_returned:
        s3_storage.get_file_names = MagicMock(return_value=s3_keys_returned)

    return s3_storage


def inspect_online_mock(mocker, html_string):
    # patch retrieve online data files
    def return_html(url, context):
        return BytesIO(html_string)

    return mocker.patch.object(urllib.request, 'urlopen', side_effect=return_html)


def request_mock(mocker, url_reponse_dict):
    def return_data(url, headers=None, params=None, auth=('username', 'password'), timeout=None):
        class MockResponse:
            def __init__(self, url_reponse_dict, status_code):
                self.url_reponse_dict = url_reponse_dict
                self.status_code = status_code

            @property
            def content(self):
                if '*' in url_reponse_dict.keys():
                    return self.url_reponse_dict.get('*')
                if url in self.url_reponse_dict.keys():
                    return self.url_reponse_dict.get(url)
                raise Exception(
                    f'no corresponding response found in api request mock for url: {url}'
                )

        return MockResponse(url_reponse_dict, 200)

    retrieved_json_mock = mocker.patch.object(requests, 'get', side_effect=return_data)
    return retrieved_json_mock


def inspect_online_selenium_mock(mocker, url_reponse_dict):

    mock = mocker.patch.object(SeleniumOnlineInspector, 'get_all_data_urls', autospec=True)
    mock.inspected_urls = []

    def get_all_data_urls(self):
        mock.inspected_urls.append(self.url)
        if self.url in url_reponse_dict.keys():
            for target_url, target_date in url_reponse_dict[self.url]:
                yield DataUrl(target_url, target_date)
        else:
            raise ValueError('No valid links found.')

    mock.side_effect = get_all_data_urls
    return mock
