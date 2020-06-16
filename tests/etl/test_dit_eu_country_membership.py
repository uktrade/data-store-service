from datatools.io.fileinfo import FileInfo

from app.etl.organisation.dit import DITEUCountryMembershipPipeline
from tests.utils import rows_equal_table


class TestDITEUCountryMembershipPipeline:
    def test_pipeline(self, app_with_db):
        pipeline = DITEUCountryMembershipPipeline(app_with_db.dbi)
        fi = FileInfo.from_path('tests/fixtures/dit/eu_country_membership/eu_spine.csv')
        pipeline.process(fi)

        # check L0
        expected_rows = [
            (
                'Austria',
                'AUT',
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
            ),
            (
                'Belgium',
                'BEL',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
            ),
            (
                'Bulgaria',
                'BGR',
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
                'EUN',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        expected_rows = [
            ('Austria', 'AUT', 1958, None),
            ('Austria', 'AUT', 1959, None),
            ('Austria', 'AUT', 1960, None),
            ('Austria', 'AUT', 1961, None),
            ('Austria', 'AUT', 1962, None),
        ]
        assert rows_equal_table(
            app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline, top_rows=5
        )
