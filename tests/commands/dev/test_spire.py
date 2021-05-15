import pytest

from app.commands.dev.spire import populate_spire_schema
from app.db.models.external import (
    SPIREApplication,
    SPIREApplicationAmendment,
    SPIREApplicationCountry,
    SPIREArs,
    SPIREBatch,
    SPIREControlEntry,
    SPIRECountryGroup,
    SPIRECountryGroupEntry,
    SPIREEndUser,
    SPIREFootnote,
    SPIREFootnoteEntry,
    SPIREGoodsIncident,
    SPIREIncident,
    SPIREMediaFootnote,
    SPIREMediaFootnoteCountry,
    SPIREMediaFootnoteDetail,
    SPIREOglType,
    SPIREReasonForRefusal,
    SPIRERefArsSubject,
    SPIRERefCountryMapping,
    SPIRERefDoNotReportValue,
    SPIRERefReportRating,
    SPIREReturn,
    SPIREThirdParty,
    SPIREUltimateEndUser,
)


class TestPopulateSpireCommand:
    @pytest.mark.parametrize(
        'batch_size,min_batch_size',
        ((1, 1), (10, 2)),
    )
    def test_populate_spire_schema(self, app_with_db, batch_size, min_batch_size):
        runner = app_with_db.test_cli_runner()
        args = ['--batch_size', batch_size, '--min_batch_size', min_batch_size]
        result = runner.invoke(populate_spire_schema, args)
        assert 'Populating schema' in result.output
        assert result.exit_code == 0
        assert result.exception is None

        # Count for some models are not batch_size because when using a
        # sub factory a new related object is created

        # Country mapping
        assert SPIRERefCountryMapping.query.count() == batch_size
        assert SPIRECountryGroup.query.count() == (batch_size * 2) + min_batch_size + 1
        assert SPIRECountryGroupEntry.query.count() == batch_size

        # Application
        assert SPIREApplication.query.count() == (batch_size * 8) + (min_batch_size * 2)
        assert SPIREApplicationAmendment.query.count() == batch_size
        assert SPIREApplicationCountry.query.count() == batch_size
        assert SPIREBatch.query.count() == (batch_size * 9) + (min_batch_size * 2) + 1

        # Ars
        assert SPIREArs.query.count() == batch_size
        assert SPIREGoodsIncident.query.count() == (batch_size * 2) + min_batch_size
        assert SPIREIncident.query.count() == batch_size
        assert SPIRERefArsSubject.query.count() == batch_size
        assert SPIREReasonForRefusal.query.count() == batch_size
        assert SPIRERefReportRating.query.count() == batch_size
        assert SPIREControlEntry.query.count() == batch_size

        # Footnotes
        assert SPIREFootnote.query.count() == batch_size + min_batch_size
        assert SPIREFootnoteEntry.query.count() == batch_size
        assert SPIREMediaFootnote.query.count() == batch_size + min_batch_size
        assert SPIREMediaFootnoteCountry.query.count() == batch_size
        assert SPIREMediaFootnoteDetail.query.count() == batch_size

        # Misc
        assert SPIRERefDoNotReportValue.query.count() == batch_size
        assert SPIREEndUser.query.count() == batch_size
        assert SPIREUltimateEndUser.query.count() == batch_size
        assert SPIREReturn.query.count() == batch_size
        assert SPIREOglType.query.count() == batch_size
        assert SPIREThirdParty.query.count() == batch_size
