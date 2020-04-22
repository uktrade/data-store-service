from unittest import mock

from app.db.models.external import SPIRECountryGroup
from app.etl.etl_spire_country_group import SPIRECountryGroupPipeline
from tests.etl.etl_rebuild_schema.test_RebuildSchemaPipeline import convert_to_csv_bytes


class TestProcess:
    def test_creation_of_model_objects(self, app_with_db):

        fieldnames = ['id']
        rows = [[0], [1]]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = SPIRECountryGroupPipeline(app_with_db.dbi)
        pipeline.process(file_info)

        session = app_with_db.db.session
        country_groups = session.query(SPIRECountryGroup).all()

        assert len(country_groups) == 2
        expected = [0, 1]
        observed = [c.id for c in country_groups]
        assert observed == expected
