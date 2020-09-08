from app.downloader.web import WebScrapingDataSource


class CompaniesHouseAccounts(WebScrapingDataSource):
    datasource = 'Companies House Accounts'
    download_url = 'http://download.companieshouse.gov.uk/en_accountsdata.html'
    download_link_regex = r'Accounts_Bulk_Data-(?P<date>\d{4}-\d{2}-\d{2})\.zip$'
    storage_file_regex = r'Accounts_Bulk_Data-(?P<date>\d{4}-\d{2}-\d{2})'
    storage_file_format = r'Accounts_Bulk_Data-{date}.zip'
