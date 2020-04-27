from datatools.io.fileinfo import FileInfo

from app.etl.organisation.dit import DITBACIPipeline
from tests.utils import rows_equal_table


class TestDITBACIPipeline:
    def test_pipeline(self, app_with_db):
        pipeline = DITBACIPipeline(app_with_db.dbi)
        fi = FileInfo.from_path('tests/fixtures/dit/baci/baci.csv')
        pipeline.process(fi)

        # check L0
        expected_rows = [(1995, 10519, 4, 251, 1.548, 0.051), (1995, 30110, 4, 381, 1.249, 0.01)]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
