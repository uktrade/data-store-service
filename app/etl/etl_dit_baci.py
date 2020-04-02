from app.etl.etl_snapshot_data import SnapshotDataPipeline


class DITBACIPipeline(SnapshotDataPipeline):
    organisation = 'dit'
    dataset = 'baci'

    _l0_data_column_types = [
        ('t', 'integer'),
        ('hs6', 'integer'),
        ('i', 'integer'),
        ('j', 'integer'),
        ('v', 'decimal'),
        ('q', 'decimal'),
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

    _l1_data_column_types = [
        ('year', 'integer'),
        ('product_category', 'integer'),
        ('exporter', 'integer'),
        ('importer', 'integer'),
        ('trade_flow_value', 'decimal'),
        ('quantity', 'decimal'),
    ]

    _l0_l1_data_transformations = {
        'year': 't',
        'product_category': 'hs6',
        'exporter': 'i',
        'importer': 'j',
        'trade_flow_value': 'v',
        'quantity': 'q',
    }
