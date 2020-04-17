from io import BytesIO

from app.etl.etl_base import DataPipeline


def _create_table(
    data_column_types, dbi, table_name, drop_existing=False, unique_column_names=None
):
    if drop_existing:
        dbi.drop_table(table_name)
    columns = ','.join(f'{c} {t}' for c, t in data_column_types)
    unique_constraint = (
        f", UNIQUE({','.join(unique_column_names)})" if unique_column_names else None
    )
    stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}{unique_constraint or ''})"
    dbi.execute_statement(stmt)


class RebuildSchemaPipeline(DataPipeline):

    '''
    Pipeline does not used the L tables to process the input data
    '''

    L0_TABLE = None
    L1_TABLE = None
    L2_TABLE = None

    data_column_types = None
    dataset = None
    organisation = None
    schema = None
    table_name = None
    unique_column_names = []

    def _create_table(self):
        return _create_table(
            self.data_column_types,
            self.dbi,
            self.table_name,
            drop_existing=True,
            unique_column_names=self.unique_column_names,
        )

    def _datafile_to_table(self, file_info):
        csv_data_no_empty_quotes = BytesIO(file_info.data.read().replace(b'""', b''))
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes,
            fq_table_name=self.table_name,
            columns=None,
            has_header=True,
            sep=',',
            quote='"',
        )

    @property
    def l1_helper_columns(self):
        ''' not used by the RebuildSchemaPipeline '''
        return None

    def process(self, file_info):
        self._create_table()
        self._datafile_to_table(file_info)
