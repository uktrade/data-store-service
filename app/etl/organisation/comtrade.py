from app.etl.pipeline_type.snapshot_data import L1SnapshotDataPipeline


class ComtradeCountryCodeAndISOPipeline(L1SnapshotDataPipeline):
    organisation = 'comtrade'
    dataset = 'country_code_and_iso'

    _l0_data_column_types = [
        ('cty_code', 'int'),
        ('cty_name_english', 'text'),
        ('cty_fullname_english', 'text'),
        ('cty_abbreviation', 'text'),
        ('cty_comments', 'text'),
        ('iso2_digit_alpha', 'text'),
        ('iso3_digit_alpha', 'text'),
        ('start_valid_year', 'text'),
        ('end_valid_year', 'text'),
    ]

    def _datafile_to_l0_temp(self, file_info):
        self.dbi.dsv_buffer_to_table(
            csv_buffer=file_info.data,
            fq_table_name=self._l0_temp_table,
            columns=None,
            has_header=True,
            sep=',',
            quote='"',
        )

    _l1_data_column_types = _l0_data_column_types

    _l0_l1_data_transformations = {}
