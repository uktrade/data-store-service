from io import BytesIO

from app.etl.pipeline_type.snapshot_data import L0SnapshotDataPipeline


class DSVToTablePipeline(L0SnapshotDataPipeline):
    def __init__(
        self, dbi, organisation, dataset, data_column_types, separator=',', quote='"', **kwargs
    ):
        super().__init__(dbi, force=True, **kwargs)
        self._organisation = organisation
        self._dataset = dataset
        self._data_column_types = data_column_types
        self.separator = separator
        self.quote = quote

    def _datafile_to_l0_temp(self, file_info):
        csv_data_no_empty_quotes = BytesIO(file_info.data.read().replace(b'""', b''))
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes,
            fq_table_name=self._l0_temp_table,
            has_header=True,
            sep=self.separator,
            quote=self.quote,
            columns=None,
        )

    @property
    def _l0_data_column_types(self):
        return self._data_column_types

    @property
    def dataset(self):
        return self._dataset

    @property
    def organisation(self):
        return self._organisation
