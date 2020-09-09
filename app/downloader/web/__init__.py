import datetime
from abc import abstractmethod

import requests

from app.downloader.abstract_datasource import AbstractDataSource
from app.downloader.inspector import OnlineInspector, StorageInspector


class WebScrapingDataSource(AbstractDataSource):
    @property
    @abstractmethod
    def download_link_regex(self):
        ...

    @property
    def _online_inspector(self):
        return OnlineInspector(
            url=self.download_url,
            download_link_regex=self.download_link_regex,
            file_date_format=self.file_date_format,
        )

    @property
    def _storage_inspector(self):
        return StorageInspector(
            storage=self.storage,
            file_regex=self.storage_file_regex,
            file_date_format=self.file_date_format,
        )

    def _get_new_online_data_url(self):
        """ Get new DataUrl if new one is available.

        Returns:
            DataUrl:
                has url of the download link, and date of that data
        Reads:
            self._storage_inspector
            self._online_inspector
        """
        online_data_url = self._online_inspector.get_latest_data_url()
        storage_date = self._storage_inspector.get_latest_date()

        new_available = (
            not storage_date
            or online_data_url.date - storage_date >= datetime.timedelta(days=self.refresh_delay)
        )
        if new_available:
            return online_data_url

    def _download_data_url(self, data_url):
        """ Downloads and stores the data specified by data_url """
        date_str = data_url.date.strftime(self.file_date_format)
        file_name = self.storage_file_format.format(date=date_str)

        r = requests.get(data_url.url)
        self.storage.write_file(file_name, r.content)

    def update(self):
        new_url = self._get_new_online_data_url()

        if new_url:
            self._download_data_url(new_url)
