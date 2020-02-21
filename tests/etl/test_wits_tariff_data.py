from datatools.io.fileinfo import FileInfo

from app.etl.etl_wits_tariff_data import WitsTariffDataPipeline

file_1 = 'tests/fixtures/wits/Product0201.csv'


class TestWitsTariffDataPipeline:
    def test_one_datafile(self, app_with_db):
        pipeline = WitsTariffDataPipeline(app_with_db.dbi)
        fi = FileInfo.from_path(file_1)
        pipeline.process(fi)
        import ipdb; ipdb.set_trace()