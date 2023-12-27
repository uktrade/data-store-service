from datatools.io.fileinfo import FileInfo

from app.etl.organisation.spire import (
    SPIREApplicationAmendmentPipeline,
    SPIREApplicationCountryPipeline,
    SPIREApplicationPipeline,
    SPIREArsPipeline,
    SPIREBatchPipeline,
    SPIREControlEntryPipeline,
    SPIRECountryGroupEntryPipeline,
    SPIRECountryGroupPipeline,
    SPIREEndUserPipeline,
    SPIREFootnoteEntryPipeline,
    SPIREFootnotePipeline,
    SPIREGoodsIncidentPipeline,
    SPIREIncidentPipeline,
    SPIREMediaFootnoteCountryPipeline,
    SPIREMediaFootnoteDetailPipeline,
    SPIREMediaFootnotePipeline,
    SPIREOglTypePipeline,
    SPIREReasonForRefusalPipeline,
    SPIRERefArsSubjectPipeline,
    SPIRERefCountryMappingPipeline,
    SPIRERefDoNotReportValuePipeline,
    SPIRERefReportRatingPipeline,
    SPIREReturnPipeline,
    SPIREThirdPartyPipeline,
    SPIREUltimateEndUserPipeline,
)


class TestProcess:
    def test_spire_pipelines(self, app_with_db):
        pipeline_datafile_mapping = [
            (SPIREBatchPipeline, 'tests/fixtures/dit/spire/batches.csv'),
            (SPIRECountryGroupPipeline, 'tests/fixtures/dit/spire/country_groups.csv'),
            (SPIREEndUserPipeline, 'tests/fixtures/dit/spire/end_users.csv'),
            (SPIREFootnotePipeline, 'tests/fixtures/dit/spire/footnotes.csv'),
            (SPIREMediaFootnotePipeline, 'tests/fixtures/dit/spire/media_footnotes.csv'),
            (
                SPIREMediaFootnoteCountryPipeline,
                'tests/fixtures/dit/spire/media_footnote_countries.csv',
            ),
            (SPIREOglTypePipeline, 'tests/fixtures/dit/spire/ogl_types.csv'),
            (SPIRERefArsSubjectPipeline, 'tests/fixtures/dit/spire/ref_ars_subjects.csv'),
            (SPIRERefCountryMappingPipeline, 'tests/fixtures/dit/spire/ref_country_mappings.csv'),
            (
                SPIRERefDoNotReportValuePipeline,
                'tests/fixtures/dit/spire/ref_do_not_report_values.csv',
            ),
            (SPIRERefReportRatingPipeline, 'tests/fixtures/dit/spire/ref_report_ratings.csv'),
            (SPIREApplicationPipeline, 'tests/fixtures/dit/spire/applications.csv'),
            (SPIRECountryGroupEntryPipeline, 'tests/fixtures/dit/spire/country_group_entries.csv'),
            (
                SPIREMediaFootnoteDetailPipeline,
                'tests/fixtures/dit/spire/media_footnote_details.csv',
            ),
            (SPIREReturnPipeline, 'tests/fixtures/dit/spire/returns.csv'),
            (
                SPIREApplicationAmendmentPipeline,
                'tests/fixtures/dit/spire/application_amendments.csv',
            ),
            (SPIREApplicationCountryPipeline, 'tests/fixtures/dit/spire/application_countries.csv'),
            (SPIREFootnoteEntryPipeline, 'tests/fixtures/dit/spire/footnote_entries.csv'),
            (SPIREGoodsIncidentPipeline, 'tests/fixtures/dit/spire/goods_incidents.csv'),
            (SPIREIncidentPipeline, 'tests/fixtures/dit/spire/incidents.csv'),
            (SPIREThirdPartyPipeline, 'tests/fixtures/dit/spire/third_parties.csv'),
            (SPIREUltimateEndUserPipeline, 'tests/fixtures/dit/spire/ultimate_end_users.csv'),
            (SPIREArsPipeline, 'tests/fixtures/dit/spire/ars.csv'),
            (SPIREControlEntryPipeline, 'tests/fixtures/dit/spire/control_entries.csv'),
            (SPIREReasonForRefusalPipeline, 'tests/fixtures/dit/spire/reasons_for_refusal.csv'),
        ]

        for pipeline, datafile in pipeline_datafile_mapping:
            fi = FileInfo.from_path(datafile)
            pipeline(app_with_db.dbi).process(fi)

            # check if table contains rows
            session = app_with_db.db.session
            rows = session.query(pipeline.sql_alchemy_model).all()
            assert len(rows) > 0
