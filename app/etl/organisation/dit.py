from io import BytesIO

from sqlalchemy import text

from app.etl.pipeline_type.incremental_data import L1IncrementalDataPipeline
from app.etl.pipeline_type.snapshot_data import L1SnapshotDataPipeline


class DITBACIPipeline(L1IncrementalDataPipeline):
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
            columns=[c for c, _ in self._l0_data_column_types],
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


class DITEUCountryMembershipPipeline(L1SnapshotDataPipeline):
    organisation = 'dit'
    dataset = 'eu_country_membership'

    _l0_data_column_types = [
        ('country', 'text'),
        ('iso3', 'text'),
        ('"1958"', 'text'),
        ('"1959"', 'text'),
        ('"1960"', 'text'),
        ('"1961"', 'text'),
        ('"1962"', 'text'),
        ('"1963"', 'text'),
        ('"1964"', 'text'),
        ('"1965"', 'text'),
        ('"1966"', 'text'),
        ('"1967"', 'text'),
        ('"1968"', 'text'),
        ('"1969"', 'text'),
        ('"1970"', 'text'),
        ('"1971"', 'text'),
        ('"1972"', 'text'),
        ('"1973"', 'text'),
        ('"1974"', 'text'),
        ('"1975"', 'text'),
        ('"1976"', 'text'),
        ('"1977"', 'text'),
        ('"1978"', 'text'),
        ('"1979"', 'text'),
        ('"1980"', 'text'),
        ('"1981"', 'text'),
        ('"1982"', 'text'),
        ('"1983"', 'text'),
        ('"1984"', 'text'),
        ('"1985"', 'text'),
        ('"1986"', 'text'),
        ('"1987"', 'text'),
        ('"1988"', 'text'),
        ('"1989"', 'text'),
        ('"1990"', 'text'),
        ('"1991"', 'text'),
        ('"1992"', 'text'),
        ('"1993"', 'text'),
        ('"1994"', 'text'),
        ('"1995"', 'text'),
        ('"1996"', 'text'),
        ('"1997"', 'text'),
        ('"1998"', 'text'),
        ('"1999"', 'text'),
        ('"2000"', 'text'),
        ('"2001"', 'text'),
        ('"2002"', 'text'),
        ('"2003"', 'text'),
        ('"2004"', 'text'),
        ('"2005"', 'text'),
        ('"2006"', 'text'),
        ('"2007"', 'text'),
        ('"2008"', 'text'),
        ('"2009"', 'text'),
        ('"2010"', 'text'),
        ('"2011"', 'text'),
        ('"2012"', 'text'),
        ('"2013"', 'text'),
        ('"2014"', 'text'),
        ('"2015"', 'text'),
        ('"2016"', 'text'),
        ('"2017"', 'text'),
        ('"2018"', 'text'),
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
        ('country', 'text'),
        ('iso3', 'text'),
        ('year', 'int'),
        ('tariff_code', 'text'),
    ]

    _l0_l1_data_transformations = {}

    def _l0_to_l1(self, datafile_name):
        l1_column_names = [c for c, _ in self._l1_column_types[1:4]]
        selection = ','.join([self._l0_l1_transformations.get(c, c) for c in l1_column_names])
        column_name_string = ','.join(l1_column_names)
        stmt = f"""
            ALTER TABLE {self._l1_table} DROP CONSTRAINT IF EXISTS
                "L1_data_source_row_id_key";
            INSERT INTO {self._l1_table} (
                {column_name_string}, year, tariff_code
            )
            SELECT
                {selection},
                key::int as year,
                value as tariff_code
            FROM (SELECT {selection}, row_to_json(t.*) AS line FROM {self._l0_table} t
                    WHERE datafile_created = '{datafile_name}') AS sq
            JOIN LATERAL json_each_text(sq.line) ON (key ~ '^[1-2]+');
        """
        self.dbi.execute_statement(text(stmt))


class DITReferencePostcodesPipeline(L1SnapshotDataPipeline):
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
