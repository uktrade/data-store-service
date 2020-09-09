import logging
from abc import ABCMeta, abstractmethod


class AbstractDataSource(metaclass=ABCMeta):
    refresh_delay = 1
    file_date_format = '%Y-%m-%d'
    logger = logging.getLogger(__name__)

    def __init__(self, storage):
        self.storage = storage

    @property
    @abstractmethod
    def storage_file_format(self):
        ...

    @property
    @abstractmethod
    def datasource(self):
        ...

    @property
    @abstractmethod
    def storage_file_regex(self):
        ...

    @property
    @abstractmethod
    def download_url(self):
        ...

    @abstractmethod
    def update(self):
        ...
