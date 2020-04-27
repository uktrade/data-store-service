from unittest import mock

from app.db.models.external import SPIRERefCountryMapping
from app.etl.organisation.spire import SPIRERefCountryMappingPipeline
from tests.etl.rebuild_schema.test_RebuildSchemaPipeline import convert_to_csv_bytes


class TestProcess:
    def test_creation_of_model_objects(self, app_with_db):

        fieldnames = ['country_id', 'country_name']
        rows = [[0, 'A'], [1, 'B']]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = SPIRERefCountryMappingPipeline(app_with_db.dbi)
        pipeline.process(file_info)

        session = app_with_db.db.session
        country_mappings = session.query(SPIRERefCountryMapping).all()

        assert len(country_mappings) == 2
        expected = [(0, 'A'), (1, 'B')]
        observed = [(c.country_id, c.country_name) for c in country_mappings]
        assert observed == expected
