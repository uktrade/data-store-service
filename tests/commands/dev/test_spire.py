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
    def test_populate_spire_schema(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        args = ['--batch_size', 1]
        result = runner.invoke(populate_spire_schema, args)
        assert 'Populating schema' in result.output
        assert result.exit_code == 0
        assert result.exception is None

        # Count for some models are not 1 because when using a
        # sub factory a new related object is created

        # Country mapping
        assert SPIRERefCountryMapping.query.count() == 1
        assert SPIRECountryGroup.query.count() == 6
        assert SPIRECountryGroupEntry.query.count() == 1

        # Application
        assert SPIREApplication.query.count() == 11
        assert SPIREApplicationAmendment.query.count() == 1
        assert SPIREApplicationCountry.query.count() == 1
        assert SPIREBatch.query.count() == 22

        # Ars
        assert SPIREArs.query.count() == 1
        assert SPIREGoodsIncident.query.count() == 4
        assert SPIREIncident.query.count() == 1
        assert SPIRERefArsSubject.query.count() == 1
        assert SPIREReasonForRefusal.query.count() == 1
        assert SPIRERefReportRating.query.count() == 2
        assert SPIREControlEntry.query.count() == 1

        # Footnotes
        assert SPIREFootnote.query.count() == 2
        assert SPIREFootnoteEntry.query.count() == 1
        assert SPIREMediaFootnote.query.count() == 3
        assert SPIREMediaFootnoteCountry.query.count() == 1
        assert SPIREMediaFootnoteDetail.query.count() == 2

        # Misc
        assert SPIRERefDoNotReportValue.query.count() == 1
        assert SPIREEndUser.query.count() == 1
        assert SPIREUltimateEndUser.query.count() == 1
        assert SPIREReturn.query.count() == 1
        assert SPIREOglType.query.count() == 1
        assert SPIREThirdParty.query.count() == 1
