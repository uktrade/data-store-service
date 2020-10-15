import datetime
import re
import ssl
import urllib
from collections import namedtuple

from bs4 import BeautifulSoup
from flask import current_app as flask_app
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.support.wait import WebDriverWait

DataUrl = namedtuple('DataUrl', 'url date')


class OnlineInspector:
    def __init__(self, url, download_link_regex, file_date_format='%Y-%m-%d'):
        self.url = url
        self.download_link_regex = download_link_regex
        self.file_date_format = file_date_format

    def get_latest_data_url(self):
        data_urls = list(self.get_all_data_urls())
        return max(data_urls, key=lambda du: du.date)

    def get_all_data_urls(self):
        with urllib.request.urlopen(self.url, context=ssl._create_unverified_context()) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        found = False
        for a in soup.find_all('a'):
            href = a.get('href')
            if href:
                match = re.match(self.download_link_regex, href)
                if match:
                    found = True
                    yield self._parse_data_url(self.url, href)
        if not found:
            raise ValueError('No valid links found.')

    def _parse_data_url(self, base_url, href):
        match = re.match(self.download_link_regex, href)
        if '?P<date>' in self.download_link_regex:
            datestr = match.group('date')
            date = datetime.datetime.strptime(datestr, self.file_date_format)
        else:
            date = None
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


class SeleniumOnlineInspector(OnlineInspector):
    def __init__(self, url, download_link_regex, download_link_xpath, file_date_format='%Y-%m-%d'):
        self.download_link_xpath = download_link_xpath
        super().__init__(url, download_link_regex, file_date_format)

    def get_all_data_urls(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.binary_location = flask_app.config['app']['chrome_binary_location']
        ignored_exceptions = (
            NoSuchElementException,
            StaleElementReferenceException,
        )
        driver = webdriver.Chrome(
            executable_path=flask_app.config['app']['chrome_driver_path'],
            chrome_options=chrome_options,
        )
        driver.get(self.url)

        found = False
        try:
            elements = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(
                lambda driver: driver.find_elements_by_xpath(self.download_link_xpath)
            )
            for element in elements:
                href = element.get_attribute("href")
                match = re.match(self.download_link_regex, href)
                if match:
                    found = True
                    yield self._parse_data_url(self.url, href)
        except TimeoutException:
            found = False
        finally:
            driver.quit()
        if not found:
            raise ValueError('No valid links found.')
