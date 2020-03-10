from app.etl.etl_snapshot_data import SnapshotDataPipeline


class WorldBankBoundRatesPipeline(SnapshotDataPipeline):
    organisation = 'world_bank'
    dataset = 'bound_rates'

    _l0_data_column_types = [
        ('nomen_code', 'text'),
        ('reporter', 'integer'),
        ('product', 'integer'),
        ('bound_rate', 'decimal'),
        ('total_number_of_lines', 'integer'),
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
