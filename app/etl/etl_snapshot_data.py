from abc import abstractmethod

from app.etl.etl_base import DataPipeline


class SnapshotDataPipeline(DataPipeline):
    """ Abstract class for standard pipelines that ingest data snapshots

    This class implements the abstract process method of a CleanDataPipeline as:
        1) parse file_info object into L0.temp (_datafile_to_l0_temp)
        2) update L0 data with L0.temp snapshot data (_l0_temp_to_l0)
        3) standardise L0 data and copy into L1 (_l0_to_l1)
        4) clean up L0.temp

    Snapshot data is stored as follows:
        - new data records are appended to existing L0, L1 records
        - L0 includes a datafile_created column to identify the file
            from which the record was originally created
        - L0 includes a datafile_updated column to track the latest
            snapshot file in which the record was present
        - L1 records are linked to L0 records by the data_source_row_id column
    """

    @property
    def l0_helper_columns(self):
        return [
            ('id', 'serial primary key'),  # primary key
            ('datafile_created', 'text'),  # first snapshot containing record
            ('datafile_updated', 'text'),  # last snapshot containing record
            ('data_hash', 'text'),         # md5 hash of the data column values, used to determine the
                                        # uniqueness of the record
        ]

    @property
    def _l0_temp_table(self):
        return self._fully_qualified(f'{self.L0_TABLE}.temp')

    @property
    def _l0_deleted_rows_table(self):
        return self._fully_qualified(f'{self.L0_TABLE}.deleted_rows')

    @property
    @abstractmethod
    def _l0_data_column_types(self):
        """ returns list of tuples containing l0 column names and
            their sql type [(l0_column_1_name, sql_type), ...]
        """
        ...

    @property
    def _l0_column_types(self):
        return self.l0_helper_columns + self._l0_data_column_types

    @property
    @abstractmethod
    def _l1_data_column_types(self):
        """ returns list of tuples containing l1 column names and
            their sql type [(l1_column_1_name, sql_type), ...]
        """
        ...

    @property
    def _l1_column_types(self):
        return self.l1_helper_columns + self._l1_data_column_types

    def process(self, file_info, delete_previous=False):
        datafile_name = file_info.name.split('/')[-1]

        self._create_table(self._l0_temp_table, self._l0_data_column_types, drop_existing=True)
        self._create_table(self._l0_table, self._l0_column_types, unique_column_names=['data_hash'])
        self._create_table(
            self._l1_table, self._l1_column_types, unique_column_names=['data_source_row_id']
        )

        self._datafile_to_l0_temp(file_info)
        self._l0_temp_to_l0(datafile_name)
        self._l0_to_l1(datafile_name)

        self.dbi.drop_table(self._l0_temp_table)

        if delete_previous:
            self._delete_from_l1(datafile_name)
            self._delete_from_l0(datafile_name)

    def _create_table(
        self, table_name, column_types, unique_column_names=None, drop_existing=False
    ):
        if drop_existing:
            self.dbi.drop_table(table_name)
        columns = ','.join(f'{c} {t}' for c, t in column_types)
        unique_constraint = (
            f", UNIQUE({','.join(unique_column_names)})" if unique_column_names else None
        )
        stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}{unique_constraint or ''})"
        self.dbi.execute_statement(stmt)

    # DATA TO L0.temp
    @abstractmethod
    def _datafile_to_l0_temp(self, file_info):
        """ Parses file_info object and populates the _l0_temp_table """
        ...

    @property
    def _l0_hash_column_indices(self):
        """ The column indices used for calculating the md5 hash to identify identical rows
        """
        return range(len(self._l0_data_column_types))

    # L0.temp TO L0
    def _l0_temp_to_l0(self, datafile_name):
        l0_data_column_names = [c for c, _ in self._l0_data_column_types]
        data_column_name_string = ','.join(l0_data_column_names)

        l0_column_names = [c for c, _ in self._l0_column_types[1:]]
        column_name_string = ','.join(l0_column_names)

        data_hash = l0_column_names[2]
        hash_columns = [l0_data_column_names[i] for i in self._l0_hash_column_indices]
        hash_columns_string = ','.join(hash_columns)

        selection = (
            ','.join([f"'{datafile_name}'"] * 2)
            + f", md5(ROW({hash_columns_string})::TEXT)"
            + ','
            + data_column_name_string
        )
        stmt = f"""
            INSERT INTO {self._l0_table}
            (
                {column_name_string}
            )
            SELECT DISTINCT ON ({hash_columns_string})
                {selection}
            FROM {self._l0_temp_table}
            ON CONFLICT ({data_hash})
            DO UPDATE SET datafile_updated = '{datafile_name}'
        """
        self.dbi.execute_statement(stmt)

    # LO TO L1
    @property
    @abstractmethod
    def _l0_l1_data_transformations(self):
        """
            returns dict of transformations {l1_column: transformation(l0_column), ...}
            to convert L0 table to L1 table. The transformations must comply with
            postgres sql syntax. If no l1 column key/value supplied, the l0 value
            will be copied as is.
        """
        ...

    @property
    def _l0_l1_transformations(self):
        """ Include transformation that references the L0 record """
        transformations = {'data_source_row_id': 'id'}
        transformations.update(self._l0_l1_data_transformations)
        return transformations

    def _l0_to_l1(self, datafile_name):
        l1_column_names = [c for c, _ in self._l1_column_types[1:]]
        selection = ','.join([self._l0_l1_transformations.get(c, c) for c in l1_column_names])
        column_name_string = ','.join(l1_column_names)
        stmt = f"""
            INSERT INTO {self._l1_table}
            (
                {column_name_string}
            )
            SELECT DISTINCT
                {selection}
            FROM {self._l0_table}
            WHERE datafile_created = '{datafile_name}'
            ON CONFLICT (data_source_row_id) DO NOTHING
        """
        self.dbi.execute_statement(stmt)

    def _delete_from_l1(self, file_name):
        delete = f"""
                    with delete_from_l0 as (
                       select id, datafile_updated
                       from {self._l0_table}
                       where datafile_updated != '{file_name}'
                    )
                    , delete_from_l1 as (
                       select
                         l1.id as l1_id,
                         l1.data_source_row_id as l0_id,
                         l0.datafile_updated
                       from delete_from_l0 l0
                        left join {self._l1_table} l1
                         on l0.id = l1.data_source_row_id
                    )
                    delete from {self._l1_table} l1
                    where exists (
                        select 1
                        from delete_from_l1 dl1
                        where dl1.l1_id = l1.id
                    )
                """
        self.dbi.execute_statement(delete)

    def _delete_from_l0(self, file_name):
        delete = f"""
                    with delete_from_l0 as (
                       select id, datafile_updated
                       from {self._l0_table}
                       where datafile_updated != '{file_name}'
                    )
                    delete from {self._l0_table} l0
                    where exists (
                        select 1
                        from delete_from_l0 dl0
                        where dl0.id = l0.id
                    )
                """
        self.dbi.execute_statement(delete)
