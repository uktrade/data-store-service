from unittest import mock

from app.db.models.external import SPIRECountryGroupEntry
from app.etl.etl_spire_country_group_entry import SPIRECountryGroupEntryPipeline
from tests.etl.etl_rebuild_schema.test_RebuildSchemaPipeline import convert_to_csv_bytes


class TestProcess:
    def test_creation_of_model_objects(self, app_with_db):

        fieldnames = ['cg_id', 'country_id']
        rows = [[0, 1], [2, 3]]
        data = convert_to_csv_bytes(fieldnames, rows)
        file_info = mock.Mock()
        file_info.data.read.return_value = data

        pipeline = SPIRECountryGroupEntryPipeline(app_with_db.dbi)
        pipeline.process(file_info)

        session = app_with_db.db.session
        country_group_entries = session.query(SPIRECountryGroupEntry).all()

        assert len(country_group_entries) == 2
        expected = [(0, 1), (2, 3)]
        observed = [(c.cg_id, c.country_id) for c in country_group_entries]
        assert observed == expected
