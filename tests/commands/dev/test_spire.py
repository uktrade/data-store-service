from app.commands.dev.spire import populate_spire_schema
from app.db.models.external import (
    SPIREApplication,
    SPIREApplicationAmendment,
    SPIREApplicationCountry,
    SPIREBatch,
    SPIRECountryGroup,
    SPIRECountryGroupEntry,
    SPIRERefCountryMapping,
)


class TestPopulateSpireCommand:
    def test_populate_spire_schema(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        args = ['--batch_size', 1]
        result = runner.invoke(populate_spire_schema, args)
        assert 'Populating schema' in result.output
        assert result.exit_code == 0
        assert result.exception is None

        # Count for some models are not 1 because when using a
        # sub factory a new related object is created

        assert SPIRERefCountryMapping.query.count() == 1
        assert SPIRECountryGroup.query.count() == 2
        assert SPIRECountryGroupEntry.query.count() == 1

        assert SPIREApplication.query.count() == 3
        assert SPIREApplicationAmendment.query.count() == 1
        assert SPIREApplicationCountry.query.count() == 1
        assert SPIREBatch.query.count() == 5
