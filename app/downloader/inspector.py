import datetime
import re
import ssl
import urllib
from collections import namedtuple

from bs4 import BeautifulSoup

DataUrl = namedtuple('DataUrl', 'url date')


class OnlineInspector:
    def __init__(self, url, download_link_regex, file_date_format='%Y-%m-%d'):
        self.url = url
        self.download_link_regex = download_link_regex
        self.file_date_format = file_date_format

    def get_latest_data_url(self):
        data_urls = list(self._get_all_data_urls())
        return max(data_urls, key=lambda du: du.date)

    def _get_all_data_urls(self):
        with urllib.request.urlopen(self.url, context=ssl._create_unverified_context()) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a'):
            href = a.get('href')
            if href:
                match = re.match(self.download_link_regex, href)
                if match:
                    yield self._parse_data_url(self.url, href)

    def _parse_data_url(self, base_url, href):
        match = re.match(self.download_link_regex, href)
        datestr = match.group('date')
        date = datetime.datetime.strptime(datestr, self.file_date_format)
        url = urllib.parse.urljoin(base_url, href)
        return DataUrl(url, date)


class StorageInspector:
    def __init__(self, storage, file_regex, file_date_format='%Y-%m-%d'):
        self.storage = storage
        self.file_regex = file_regex
        self.file_date_format = file_date_format

    def get_objects(self):
        for f in self.storage.get_file_names():
            if re.search(self.file_regex, f):
                yield f

    def _object_name_to_date(self, name):
        match = re.search(self.file_regex, name)
        datestr = match.group('date')
        return datetime.datetime.strptime(datestr, self.file_date_format)

    def get_all_dates(self):
        for o in self.get_objects():
            yield self._object_name_to_date(o)

    def get_latest_date(self):
        dates = list(self.get_all_dates())
        if not dates:
            return None
        else:
            return max(dates)
