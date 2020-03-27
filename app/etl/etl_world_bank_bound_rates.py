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

    _l1_data_column_types = [
        ('reporter', 'integer'),
        ('product', 'integer'),
        ('bound_rate', 'decimal'),
    ]

    _l0_l1_data_transformations = {}

    def _l0_to_l1(self, datafile_name):
        """
        This is overridden because the source file contains multiple bound rates
        for the same reporter/product combination. This function selects the
        bound rate with the highest nomen code for each combination.
        """
        l1_column_names = [c for c, _ in self._l1_column_types[1:]]
        selection = ','.join([self._l0_l1_transformations.get(c, c) for c in l1_column_names])
        column_name_string = ','.join(l1_column_names)
        grouping = 'reporter, product'
        stmt = f"""
            INSERT INTO {self._l1_table}
            (
                {column_name_string}
            )
            SELECT DISTINCT ON ({grouping})
                {selection}
            FROM {self._l0_table}
            WHERE datafile_created = '{datafile_name}'
            ORDER BY {grouping}, RIGHT(nomen_code,1)::int DESC
            ON CONFLICT (data_source_row_id) DO NOTHING
        """
        self.dbi.execute_statement(stmt)
