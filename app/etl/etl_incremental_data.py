from abc import abstractmethod

from app.etl.etl_base import DataPipeline


class IncrementalDataPipeline(DataPipeline):
    """ Abstract class for standard pipelines that ingests data incrementally
    (i.e. not snapshot data)

    This class implements the abstract process method of a CleanDataPipeline as:
        1) parse file_info object and copy into L0.temp (datafile_to_l0_temp)
        2) standardise L0.temp data and copy into L1.temp (l0_to_l1)
        3) append L0.temp to existing L0 data (append_l0_temp_to_l0) and drop L0.temp
        4) append L1.temp to existing L1 data and drop L1.temp

    Because of the incremental data nature, temporary tables (L0.temp, L1.temp) are used
    to process the current data.
    At the end of the pipeline the temporary data is merged with existing L0/L1 data and cleaned up.

    """

    @property
    def l0_helper_columns(self):
        return [
            ('id', f"int primary key default nextval('{self._l0_sequence}')"),  # primary key
            ('datafile_created', 'text'),  # datafile_created
        ]

    @property
    def _l0_temp_table(self):
        return self._fully_qualified(f'{self.L0_TABLE}.temp')

    @property
    def _l0_sequence(self):
        return f'"{self.schema}"."{self.L0_TABLE}_SEQUENCE"'

    @property
    def _l1_temp_table(self):
        return self._fully_qualified(f'{self.L1_TABLE}.temp')

    @property
    def _l1_sequence(self):
        return f'"{self.schema}"."{self.L1_TABLE}_SEQUENCE"'

    @property
    @abstractmethod
    def _l0_data_column_types(self):
        """ returns list of tuples containing l0 column names and their sql
            type [(l0_column_1_name, sql_type), ...]
        """
        ...

    @property
    def _l0_column_types(self):
        return self.l0_helper_columns + self._l0_data_column_types

    @property
    @abstractmethod
    def _l1_data_column_types(self):
        """ returns list of tuples containing l1 column names and their sql
            type [(l1_column_1_name, sql_type), ...]
        """
        ...

    @property
    def _l1_column_types(self):
        return self.l1_helper_columns + self._l1_data_column_types

    def create_tables(self):
        self._create_sequence(self._l0_sequence)
        self._create_sequence(self._l1_sequence)

        self._create_table(self._l0_table, self._l0_column_types, drop_existing=self.force)
        self._create_table(self._l1_table, self._l1_column_types, drop_existing=self.force)
        self._create_table(self._l0_temp_table, self._l0_column_types, drop_existing=True)
        self._create_table(self._l1_temp_table, self._l1_column_types, drop_existing=True)

    def process(self, file_info=None, drop_source=True):
        self.create_tables()
        self._datafile_to_l0_temp(file_info)
        self._l0_to_l1()

        if file_info:  # append and include filename in target table
            datafile_name = file_info.name.split('/')[-1] if file_info else None
            self.append_l0_temp_to_l0(datafile_name=datafile_name)
            self.dbi.drop_table(self._l0_temp_table)
        else:  # append as is
            self.dbi.append_table(self._l0_temp_table, self._l0_table, drop_source=True)
        self.dbi.append_table(source_table=self._l1_temp_table, target_table=self._l1_table)

    def _create_table(self, table_name, column_types, drop_existing=False):
        if drop_existing:
            self.dbi.drop_table(table_name)
        columns = ','.join(f'{c} {t}' for c, t in column_types)
        stmt = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})'
        self.dbi.execute_statement(stmt)

    def _create_sequence(self, sequence_name):
        stmt = f'CREATE SEQUENCE IF NOT EXISTS {sequence_name}'
        self.dbi.execute_statement(stmt)

    # DATA TO L0.temp
    @abstractmethod
    def _datafile_to_l0_temp(self, file_info):
        """ Parses file_info object and populates the _l0_temp_table """
        ...

    # LO.temp TO L1.temp
    @property
    @abstractmethod
    def _l0_l1_data_transformations(self):
        """
            returns dict of transformations {l1_column: transformation(l0_column), ...}
            to convert L0 table to L1 table. The transformations must comply with postgres
            sql syntax. If no l1 column key/value supplied, the l0 value will be copied as is.
        """
        ...

    @property
    def _l0_l1_transformations(self):
        """ Include transformation for id and a data_source_row_id that references the L0 record """
        transformations = {'id': f"nextval('{self._l1_sequence}')", 'data_source_row_id': 'id'}
        transformations.update(self._l0_l1_data_transformations)
        return transformations

    def _l0_to_l1(self):
        l1_column_names = [c for c, _ in self._l1_column_types]
        selection = ','.join([self._l0_l1_transformations.get(c, c) for c in l1_column_names])
        column_name_string = ','.join(l1_column_names)
        stmt = f"""
            INSERT INTO {self._l1_temp_table}
            (
                {column_name_string}
            )
            SELECT
                {selection}
            FROM {self._l0_temp_table}
        """
        self.dbi.execute_statement(stmt)

    # append L0.temp TO L0
    def append_l0_temp_to_l0(self, datafile_name):
        l0_data_column_names = [c for c, _ in self._l0_data_column_types]
        data_column_name_string = ','.join(l0_data_column_names)

        l0_column_names = [c for c, _ in self._l0_column_types]
        column_name_string = ','.join(l0_column_names)

        selection = f"id, '{datafile_name}', {data_column_name_string}"
        stmt = f"""
            INSERT INTO {self._l0_table}
            (
                {column_name_string}
            )
            SELECT
                {selection}
            FROM {self._l0_temp_table}
        """
        self.dbi.execute_statement(stmt)
