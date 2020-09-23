from app.downloader.web import WebScrapingDataSource


class HMRCExporters(WebScrapingDataSource):
    datasource = 'HMRC Exporters'
    download_url = 'https://www.uktradeinfo.com/Statistics/Pages/DataDownloads.aspx'
    download_link_regex = r'(.*?)/exporters(?P<date>\d{4})\.zip$'
    file_date_format = '%y%m'
    storage_file_regex = r'exporters(?P<date>\d{4})'
    storage_file_format = r'exporters{date}.zip'
