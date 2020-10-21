from io import BytesIO

from app.etl.pipeline_type.incremental_data import L0IncrementalDataPipeline
from app.utils import trigger_dataflow_dag


class DSVToTablePipeline(L0IncrementalDataPipeline):
    def __init__(
        self, dbi, organisation, dataset, data_column_types, separator=',', quote='"', **kwargs
    ):
        self.organisation = organisation
        self.dataset = dataset
        self._data_column_types = self._format_column_names(data_column_types)
        self.separator = separator
        self.quote = quote
        super().__init__(dbi, force=True, **kwargs)

    def _datafile_to_l0_temp(self, file_info):
        csv_data_no_empty_quotes = BytesIO(file_info.data.read().replace(b'""', b''))
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes,
            fq_table_name=self._l0_temp_table,
            has_header=True,
            sep=self.separator,
            quote=self.quote,
            columns=[c for c, _ in self._l0_data_column_types],
        )

    def _format_column_names(self, column_types):
        formatted_column_types = []
        for name, type in column_types:
            formatted_column_types.append((f'''"{name.strip('"')}"''', type))
        return formatted_column_types

    @property
    def _l0_data_column_types(self):
        return self._data_column_types

    def trigger_dataflow_dag(self):
        return trigger_dataflow_dag(self.schema, self.L0_TABLE)
