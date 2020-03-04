from io import BytesIO

from app.etl.etl_snapshot_data import SnapshotDataPipeline


class DITReferencePostcodesPipeline(SnapshotDataPipeline):
    organisation = 'dit'
    dataset = 'reference_postcodes'

    _l0_data_column_types = [
        ("postcode", "text"),
        ("local_authority_district_code", "text"),
        ("local_authority_district_name", "text"),
        ("local_enterprise_partnership_lep1_code", "text"),
        ("local_enterprise_partnership_lep1_name", "text"),
        ("local_enterprise_partnership_lep2_code", "text"),
        ("local_enterprise_partnership_lep2_name", "text"),
        ("region_code", "text"),
        ("region_name", "text"),
        ("national_grid_ref_easting", "text"),
        ("national_grid_ref_northing", "text"),
        ("date_of_introduction", "date"),
        ("date_of_termination", "date"),
    ]

    def _datafile_to_l0_temp(self, file_info):
        csv_data_no_empty_quotes = BytesIO(file_info.data.read().replace(b'""', b''))
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes,
            fq_table_name=self._l0_temp_table,
            columns=None,
            has_header=True,
            sep=',',
            quote='"',
        )

    _l1_data_column_types = _l0_data_column_types
    _l0_l1_data_transformations = {}
