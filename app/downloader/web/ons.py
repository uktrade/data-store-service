from app.downloader.web import ONSGeoPortalWebScrapingDataSource


class ONSPostcodeDirectory(ONSGeoPortalWebScrapingDataSource):
    datasource = 'ONS Postcode Directory'
    download_url = (
        'https://geoportal.statistics.gov.uk/search?sort=-created&tags=ons%20postcode%20directory'
    )
    download_link_regex = r'(.*)/datasets/ons-postcode-directory-(?P<date>[a-zA-Z]+-\d{4})$'
    download_link_date_format = '%B-%Y'
    download_link_xpath = "//a[contains(@id, 'search-result-element')]"
    storage_file_regex = r'ONSPD_(?P<date>\w{3}_\d{4})\_UK.zip$'
    storage_file_format = r'ONSPD_{date}_UK.zip'
    storage_file_date_format = '%b_%Y'
