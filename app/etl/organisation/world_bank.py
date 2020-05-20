import time
from io import BytesIO
from multiprocessing.pool import ThreadPool as Pool

import psycopg2
from flask import current_app as flask_app

from app.etl.organisation.comtrade import ComtradeCountryCodeAndISOPipeline
from app.etl.organisation.dit import DITBACIPipeline
from app.etl.organisation.dit import DITEUCountryMembershipPipeline
from app.etl.pipeline_type.incremental_data import IncrementalDataPipeline
from app.etl.pipeline_type.snapshot_data import L1SnapshotDataPipeline


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r  %2.2f min' % (method.__name__, (te - ts) / 60))
        return result

    return timed


class WorldBankBoundRatesPipeline(L1SnapshotDataPipeline):
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


class WorldBankTariffPipeline(IncrementalDataPipeline):
    organisation = 'world_bank'
    dataset = 'tariff'

    _l0_data_column_types = [
        ('reporter', 'integer'),
        ('year', 'integer'),
        ('product', 'integer'),
        ('partner', 'integer'),
        ('duty_type', 'text'),
        ('simple_average', 'decimal'),
        ('number_of_total_lines', 'integer'),
    ]

    _l1_data_column_types = None

    def process(self, file_info=None, drop_source=True, **kwargs):
        self._create_sequence(self._l0_sequence, drop_existing=self.options.force)
        self._create_table(self._l0_table, self._l0_column_types, drop_existing=self.options.force)
        self._create_table(self._l0_temp_table, self._l0_column_types, drop_existing=True)
        self._datafile_to_l0_temp(file_info)
        datafile_name = file_info.name.split('/')[-1] if file_info else None
        self.append_l0_temp_to_l0(datafile_name=datafile_name)
        self.dbi.drop_table(self._l0_temp_table)

    def _datafile_to_l0_temp(self, file_info):
        csv_data_no_empty_quotes = BytesIO(file_info.data.read().replace(b'""', b''))
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes,
            fq_table_name=self._l0_temp_table,
            columns=[c for c, _ in self._l0_data_column_types],
            has_header=True,
            sep=',',
            quote='"',
        )

    _l0_l1_data_transformations = {}

    def _l0_to_l1(self):
        pass


class WorldBankTariffTransformPipeline(IncrementalDataPipeline):
    organisation = WorldBankTariffPipeline.organisation
    dataset = WorldBankTariffPipeline.dataset
    subdataset = 'transformed'

    _l0_data_column_types = None

    l1_helper_columns = [
        ('id', 'bigserial primary key'),
    ]

    _l1_data_column_types = [
        ('product', 'integer'),
        ('reporter', 'text'),
        ('partner', 'text'),
        ('year', 'integer'),
        ('assumed_tariff', 'decimal'),
        ('app_rate', 'decimal'),
        ('mfn_rate', 'decimal'),
        ('bnd_rate', 'decimal'),
        ('eu_rep_rate', 'decimal'),
        ('eu_part_rate', 'decimal'),
        ('country_average', 'decimal'),
        ('world_average', 'decimal'),
    ]

    def set_option_defaults(self, options):
        options.setdefault('continue_transform', False)
        options.setdefault('products', None)
        return super().set_option_defaults(options)

    def _datafile_to_l0_temp(self, file_info):
        pass

    _l0_l1_data_transformations = {}

    def process(self, file_info=None, drop_source=True, **kwargs):
        drop_existing = not self.options.continue_transform
        self._create_table(self._l1_temp_table, self._l1_column_types, drop_existing=drop_existing)
        self._l0_to_l1()
        self.create_indices()
        self.finish_processing()

    def create_indices(self):
        for field in ['product', 'reporter', 'partner', 'year']:
            stmt = f"""
                CREATE INDEX IF NOT EXISTS "{self.L1_TABLE}.temp_{field}_idx"
                ON {self._l1_temp_table} USING hash ({field});
            """
            self.dbi.execute_statement(stmt, raise_if_fail=True)

    def finish_processing(self):
        # swap tables to minimize api downtime
        self.dbi.drop_table(self._l1_table)
        self.dbi.rename_table(
            self.schema,
            self.dbi.parse_fully_qualified(self._l1_temp_table).table,
            self.dbi.parse_fully_qualified(self._l1_table).table,
        )

    required_countries_vn = 'required_countries'
    all_tariffs_vn = 'all_tariffs'
    baci_spine_vn = 'baci_spine'
    eu_countries_vn = 'eu_countries'
    eu_reporter_rates_vn = 'eu_reporter_rates'
    tariffs_with_eu_reporter_rates_vn = 'tariffs_with_eu_reporter_rates'
    eu_partner_rates_vn = 'eu_partner_rates'
    tariffs_with_world_partner_rates_vn = 'tariffs_with_world_partner_rates'
    cutoff_year = 2000

    @timeit
    def _l0_to_l1(self):
        views = [
            (self.required_countries_vn, self._create_required_countries_view),
            (self.baci_spine_vn, self._create_baci_spine_view),
            (self.all_tariffs_vn, self._create_all_tariffs_view),
            (self.eu_countries_vn, self._create_eu_countries_view),
            (self.eu_reporter_rates_vn, self._create_eu_reporter_rates_view),
            (
                self.tariffs_with_eu_reporter_rates_vn,
                self._create_tariffs_with_eu_reporter_rates_view,
            ),
            (self.eu_partner_rates_vn, self._create_eu_partner_rates_view),
            (
                self.tariffs_with_world_partner_rates_vn,
                self._create_tariffs_with_world_partner_rates_view,
            ),
        ]
        for view_name, create_view in views:
            fq_view_name = self._fq(view_name)
            if self.options.force:
                self.dbi.drop_materialized_view(fq_view_name)
            if not self.dbi.table_exists(self.schema, view_name, materialized_view=True):
                create_view()
            else:
                self.dbi.refresh_materialised_view(fq_view_name)
        products = self._get_products()
        p = Pool(processes=10)
        connection_str = str(flask_app.db.engine.url)
        for i, product in enumerate(products):
            print(f'Processing product: {i + 1}/{len(products)}', end='\t')
            code = str(product[0])
            p.apply_async(
                self._clean_and_transform_tariffs,
                (code, connection_str, WorldBankBoundRatesPipeline(self.dbi)._l1_table,),
            )
        p.close()
        p.join()
        print('Done!')

    def _fq(self, table_name):
        return self.dbi.to_fully_qualified(table_name, self.schema)

    def format_product_option(self, product_string):
        products = product_string.split(',')
        try:
            list(map(int, products))
        except ValueError or SyntaxError:
            print(f'Product string invalid - {product_string}')
            return []
        return products

    def get_where_products_clause(self):
        where = ""
        where_clauses = []
        if self.options.continue_transform or self.options.products:

            if self.options.continue_transform:
                where_clauses.append(
                    f"product not in (select distinct product from {self._l1_temp_table})"
                )
            if self.options.products:
                products = self.format_product_option(self.options.products)
                if len(products) == 1:
                    where_clauses.append(f"product = {products[0]}")
                elif len(products) > 1:
                    where_clauses.append(f"product in {tuple(products)}")
            if where_clauses:
                where = 'where ' + ' and '.join(where_clauses)
        return where

    def _get_products(self):
        where = self.get_where_products_clause()
        stmt = f"""
        select distinct
            product
        from {self._fq(self.baci_spine_vn)}
        {where}
        order by product
        """
        return self.dbi.execute_query(stmt, raise_if_fail=True)

    @timeit
    def _clean_and_transform_tariffs(
        self, product, connection_str, world_bank_bound_rates_l1_table
    ):
        conn, curs = None, None
        try:
            stmt = f"""
            with bnd_tariffs as (
                select
                    t1.product,
                    t2.iso3 as reporter,
                    bound_rate as bnd_rate
                from {world_bank_bound_rates_l1_table} t1
                left join {self._fq(self.required_countries_vn)} t2
                    on t1.reporter = t2.iso_num
                where t2.requirement is not null
            ), product_tariffs as (
                select distinct
                    product,
                    year,
                    reporter,
                    partner,
                    app_rate,
                    t3.mfn_rate,
                    t4.bnd_rate,
                    eu_rep,
                    eu_rep_rate,
                    eu_part,
                    eu_part_rate
                from (
                    select * from {self._fq(self.tariffs_with_eu_reporter_rates_vn)}
                    where product = '{product}'
                ) t1
                left join (
                    select * from {self._fq(self.eu_partner_rates_vn)}
                    where product = '{product}'
                ) t2 using (product, year, reporter, eu_part)
                left join (
                    select * from {self._fq(self.tariffs_with_world_partner_rates_vn)}
                    where product = '{product}'
                ) t3 using (product, year, partner, reporter)
                left join (
                    select * from bnd_tariffs
                    where product = '{product}'
                ) t4 using (product, reporter)
                where partner is not null
            ), cleaned_tariffs as (
                select
                    product,
                    year,
                    reporter,
                    partner,
                    app_rate,
                    mfn_rate,
                    bnd_rate,
                    eu_rep_rate,
                    eu_part_rate,
                    avg(app_rate) OVER () as world_average,
                    avg(app_rate) OVER (PARTITION BY reporter) as country_average,
                    coalesce(
                        app_rate,
                        eu_rep_rate,
                        eu_part_rate,
                        mfn_rate
                    ) as final_tariff
                from product_tariffs
            ), filled_tariffs as (
                select
                    *,
                    first_value(final_tariff)
                        OVER (
                            PARTITION BY reporter, partner,
                            final_tariff_partition ORDER BY year
                        )
                    as filled_tariff
                from (
                    select
                        *,
                        count(final_tariff)
                            over (partition by reporter, partner ORDER BY year)
                        as final_tariff_partition
                    from cleaned_tariffs
                ) sq
            ), assumed_tariffs as (
                select *, coalesce(filled_tariff, country_average, world_average) as assumed_tariff
                from filled_tariffs
            )
            insert into {self._l1_temp_table} (
                product,
                reporter,
                partner,
                year,
                assumed_tariff,
                app_rate,
                mfn_rate,
                bnd_rate,
                eu_rep_rate,
                eu_part_rate,
                country_average,
                world_average
            )
            select
                {product} as product,
                reporter,
                partner,
                year,
                round(avg(assumed_tariff), 3) as assumed_tariff,
                round(avg(app_rate), 3) as app_rate,
                round(avg(mfn_rate), 3) as mfn_rate,
                round(avg(bnd_rate), 3) as bnd_rate,
                round(avg(eu_rep_rate), 3) as eu_rep_rate,
                round(avg(eu_part_rate), 3) as eu_part_rate,
                round(avg(country_average), 3) as country_average,
                round(avg(world_average), 3) as world_average
            from assumed_tariffs t1
            group by reporter, partner, year
            order by reporter, partner, year;
            """
            conn = psycopg2.connect(connection_str)
            curs = conn.cursor()
            curs.execute(stmt)
            conn.commit()
        finally:
            if curs:
                curs.close()
            if conn:
                conn.close()

    @timeit
    def _create_required_countries_view(self):
        stmt = f"""
        create materialized view if not exists {self._fq(self.required_countries_vn)} as (
            with required_countries (iso3, iso_num,	requirement) as (values
                ('ABW',	533, True),
                ('AFG',	4, True),
                ('AGO',	24, True),
                ('ALB',	8, True),
                ('ARE',	784, True),
                ('ARG',	32, True),
                ('ARM',	51, True),
                ('ATG',	28, True),
                ('AUS',	36, True),
                ('AUT',	40, True),
                ('AZE',	31, True),
                ('BDI',	108, True),
                ('BEL',	56, True),
                ('BEN',	204, True),
                ('BFA',	854, True),
                ('BGD',	50, True),
                ('BGR',	100, True),
                ('BHR',	48, True),
                ('BHS',	44, True),
                ('BIH',	70, True),
                ('BLR',	112, True),
                ('BLZ',	84, True),
                ('BOL',	68, True),
                ('BRA',	76, True),
                ('BRB',	52, True),
                ('BRN',	96, True),
                ('BTN',	64, True),
                ('CAF',	140, True),
                ('CAN',	124, True),
                ('CHE',	756, True),
                ('CHL',	152, True),
                ('CHN',	156, True),
                ('CIV',	384, True),
                ('CMR',	120, True),
                ('COD',	180, True),
                ('COG',	178, True),
                ('COL',	170, True),
                ('COM',	174, True),
                ('CPV',	132, True),
                ('CRI',	188, True),
                ('CYP',	196, True),
                ('CZE',	203, True),
                ('DEU',	276, True),
                ('DJI',	262, True),
                ('DMA',	212, True),
                ('DNK',	208, True),
                ('DOM',	214, True),
                ('DZA',	12, True),
                ('ECU',	218, True),
                ('EGY',	818, True),
                ('ERI',	232, True),
                ('ESP',	724, True),
                ('EST',	233, True),
                ('ETH',	231, True),
                ('FIN',	246, True),
                ('FJI',	242, True),
                ('FRA',	250, True),
                ('FRA',	251, True),
                ('FSM',	583, True),
                ('GAB',	266, True),
                ('GBR',	826, True),
                ('GEO',	268, True),
                ('GHA',	288, True),
                ('GIN',	324, True),
                ('GMB',	270, True),
                ('GNB',	624, True),
                ('GNQ',	226, True),
                ('GRC',	300, True),
                ('GRD',	308, True),
                ('GTM',	320, True),
                ('GUY',	328, True),
                ('HKG',	344, True),
                ('HND',	340, True),
                ('HRV',	191, True),
                ('HTI',	332, True),
                ('HUN',	348, True),
                ('IDN',	360, True),
                ('IND',	699, True),
                ('IRL',	372, True),
                ('IRN',	364, True),
                ('IRQ',	368, True),
                ('ISL',	352, True),
                ('ISR',	376, True),
                ('ITA',	381, True),
                ('ITA',	380, True),
                ('JAM',	388, True),
                ('JOR',	400, True),
                ('JPN',	392, True),
                ('KAZ',	398, True),
                ('KEN',	404, True),
                ('KGZ',	417, True),
                ('KHM',	116, True),
                ('KIR',	296, True),
                ('KNA',	659, True),
                ('KOR',	410, True),
                ('KWT',	414, True),
                ('LAO',	418, True),
                ('LBN',	422, True),
                ('LBR',	430, True),
                ('LBY',	434, True),
                ('LCA',	662, True),
                ('LKA',	144, True),
                ('LTU',	440, True),
                ('LVA',	428, True),
                ('MAC',	446, True),
                ('MAR',	504, True),
                ('MDA',	498, True),
                ('MDG',	450, True),
                ('MDV',	462, True),
                ('MEX',	484, True),
                ('MHL',	584, True),
                ('MKD',	807, True),
                ('MLI',	466, True),
                ('MLT',	470, True),
                ('MMR',	104, True),
                ('MNG',	496, True),
                ('MOZ',	508, True),
                ('MRT',	478, True),
                ('MUS',	480, True),
                ('MWI',	454, True),
                ('MYS',	458, True),
                ('NER',	562, True),
                ('NGA',	566, True),
                ('NIC',	558, True),
                ('NLD',	528, True),
                ('NOR',	579, True),
                ('NPL',	524, True),
                ('NRU',	520, True),
                ('NZL',	554, True),
                ('OMN',	512, True),
                ('PAK',	586, True),
                ('PAN',	591, True),
                ('PER',	604, True),
                ('PHL',	608, True),
                ('PLW',	585, True),
                ('PNG',	598, True),
                ('POL',	616, True),
                ('PRT',	620, True),
                ('PRY',	600, True),
                ('QAT',	634, True),
                ('ROU',	642, True),
                ('RUS',	643, True),
                ('RWA',	646, True),
                ('SAU',	682, True),
                ('SDN',	729, True),
                ('SEN',	686, True),
                ('SGP',	702, True),
                ('SLB',	90, True),
                ('SLE',	694, True),
                ('SLV',	222, True),
                ('SMR',	674, True),
                ('SOM',	706, True),
                ('STP',	678, True),
                ('SUR',	740, True),
                ('SVK',	703, True),
                ('SVN',	705, True),
                ('SWE',	752, True),
                ('SYC',	690, True),
                ('TCD',	148, True),
                ('TGO',	768, True),
                ('THA',	764, True),
                ('TJK',	762, True),
                ('TKM',	795, True),
                ('TON',	776, True),
                ('TTO',	780, True),
                ('TUN',	788, True),
                ('TUR',	792, True),
                ('TUV',	798, True),
                ('TWN',	490, True),
                ('TZA',	834, True),
                ('UGA',	800, True),
                ('UKR',	804, True),
                ('URY',	858, True),
                ('USA',	842, True),
                ('UZB',	860, True),
                ('VCT',	670, True),
                ('VEN',	862, True),
                ('VNM',	704, True),
                ('VUT',	548, True),
                ('WSM',	882, True),
                ('YEM',	887, True),
                ('ZAF',	710, True),
                ('ZMB',	894, True),
                ('ZWE',	716, True)
            ) select
                iso3,
                iso_num,
                requirement
            from required_countries
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_baci_spine_view(self):
        """
            Create spine as cross product of product, year, reporter and partner
            combinations from BACI data joined on required country and filtered on
                - only containing required country pairs
                - reporter must be different from partner
            Switch to use iso3 instead of iso_number for partner/reporter

            Used by (eu_reporter_rates_view, tariffs_with_eu_reporter_rates)
        """
        dit_baci = DITBACIPipeline(self.dbi)
        stmt = f"""
        create materialized view if not exists {self._fq(self.baci_spine_vn)} as (
            with baci_master as (
                select distinct
                    product_category as product,
                    importer as reporter,
                    exporter as partner,
                    year
                from {dit_baci._l1_table}
                where year >= {self.cutoff_year}
            ), baci_master_required_countries as (
                select
                    product,
                    year,
                    t2.iso3 as reporter,
                    t3.iso3 as partner
                from  baci_master t1
                left join {self._fq(self.required_countries_vn)} t2
                    on t1.reporter = t2.iso_num
                left join {self._fq(self.required_countries_vn)} t3
                    on t1.partner = t3.iso_num
                where t2.requirement is True and t3.requirement is True
            )
            select distinct
                product,
                year,
                reporter,
                partner
            from (select distinct reporter from baci_master_required_countries
                where reporter is not null) t1
            cross join (select distinct partner from baci_master_required_countries
                where partner is not null) cj1
            cross join (select distinct year from baci_master_required_countries
                where year is not null) cj2
            cross join (select distinct product from baci_master_required_countries
                where product is not null) cj3
            where reporter != partner
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_all_tariffs_view(self):
        """
            Create tariff view with different types as columns. Switch to use
            iso3 instead of iso_number for partner/reporter

            Used by (eu_reporter_rates_view, tariffs_with_eu_reporter_rates)

        """
        stmt = f"""
        create materialized view if not exists {self._fq(self.all_tariffs_vn)} as (
            with tariffs_and_countries as (
                select
                    t1.product,
                    t1.year as year,
                    t1.reporter as rep_iso_num,
                    t2.iso3 as rep_iso3,
                    t2.requirement as rep_req,
                    t1.partner as part_iso_num,
                    t3.iso3 as part_iso3,
                    t3.requirement as part_req,
                    t1.duty_type as tariff_type,
                    t1.simple_average
                from {self._l0_table} t1
                left join {self._fq(self.required_countries_vn)} t2
                    on t1.reporter = t2.iso_num
                left join {self._fq(self.required_countries_vn)} t3
                    on t1.partner = t3.iso_num
            ), ahs_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'AHS'
            ), mfn_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'MFN'
            )
            select distinct
                product,
                year,
                coalesce(t1.rep_iso_num, t2.rep_iso_num) as rep_iso_num,
                coalesce(t1.rep_iso3, t2.rep_iso3) as reporter,
                coalesce(t1.rep_req, t2.rep_req) as rep_req,
                coalesce(t1.part_iso_num, t2.part_iso_num) as part_iso_num,
                t1.part_iso3 as partner,
                    -- always null for t2
                t1.part_req as part_req,
                    -- always null for t2
                t1.simple_average as app_rate,
                t2.simple_average as mfn_rate
            from ahs_tariffs t1
            full join mfn_tariffs t2 using (product, year, rep_iso3, part_iso3) -- only 0 partner
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_eu_countries_view(self):
        """
            Eu country list filtered on
                - only containing required country pairs
                - tariff_code is EUN

            Used by (eu_reporter_rates)
        """
        dit_eu_country_membership = DITEUCountryMembershipPipeline(self.dbi)
        comtrade_country_code_and_iso = ComtradeCountryCodeAndISOPipeline(self.dbi)
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_countries_vn)} as (

            select distinct
                t1.year,
                t1.iso3,
                t1.tariff_code
            from {dit_eu_country_membership._l1_table} t1
            left join {self._fq(self.required_countries_vn)} t2
                using (iso3)
            where requirement is True and tariff_code = 'EUN'
            order by t1.iso3, t1.year

            -- select distinct
            --    year,
            --    t1.iso3,
            --    tariff_code
            -- from {dit_eu_country_membership._l1_table} t1
            -- left join {comtrade_country_code_and_iso._l1_table} t2
            --        on t1.iso3 = t2.iso3_digit_alpha
            -- left join {self._fq(self.required_countries_vn)} t3
            --        on t1.iso3 = t3.iso3 and t2.cty_code = t3.iso_num
            -- where tariff_code = 'EUN' and requirement is True
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_eu_reporter_rates_view(self):
        """
            All tariffs where
                - reporter is EU (918)
                - partner is part of spine
            expanded to include all eu countries
                - EU-EU rates set to 0 if no app rate present

            Only a reporter can be 918 and a corresponding partner can only be non-eu

            Used by (tariffs_with_eu_reporter_rates)
        """
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_reporter_rates_vn)} as (
            with eu_reported_tariffs as (
                select distinct
                    product,
                    year,
                    partner,
                    app_rate
                from {self._fq(self.baci_spine_vn)}
                left join {self._fq(self.all_tariffs_vn)}
                    using (product, year, partner)
                where part_req = True and rep_iso_num = 918
            ), eu_product_spine as (
                select distinct
                    product,
                    t1.year,
                    t2.iso3,
                    t2.tariff_code
                from eu_reported_tariffs t1
                cross join {self._fq(self.eu_countries_vn)} t2
            ), expanded_with_eu_partners as (
                select product, t1.year, partner, t2.tariff_code from eu_reported_tariffs t1
                left join {self._fq(self.eu_countries_vn)} t2
                    on t1.partner = t2.iso3 and t1.year = t2.year
                union
                select product, year, iso3 as partner, tariff_code from eu_product_spine
            ), expanded_with_eu_partners_and_reporters as (
                select
                    product,
                    year,
                    partner,
                    t2.iso3 as reporter,
                    t1.tariff_code as eu_part,
                    t2.tariff_code as eu_rep
                from expanded_with_eu_partners t1
                left join {self._fq(self.eu_countries_vn)} t2 using (year)
                where partner != t2.iso3
            )
            select distinct
                product,
                year,
                reporter,
                partner,
                eu_rep,
                eu_part,
                case
                    when
                        app_rate is null
                        and eu_rep = 'EUN'
                        and eu_part = 'EUN'
                    then 0
                    -- set all EU - EU rates to 0
                    else app_rate
                end as eu_rep_rate
            from eu_reported_tariffs
            full join expanded_with_eu_partners_and_reporters
            using (product, year, partner)
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_tariffs_with_eu_reporter_rates_view(self):
        """
            All required tariffs including the eu reported rates
            and if partner/reporter is EUN

            Used by (eu_partner_rates, product_tariffs)
        """
        stmt = f"""
        create materialized view if not exists
            {self._fq(self.tariffs_with_eu_reporter_rates_vn)} as (
            with all_tariffs as (
                select distinct
                    product,
                    year,
                    reporter,
                    partner,
                    app_rate
                from {self._fq(self.baci_spine_vn)} t1
                left join {self._fq(self.all_tariffs_vn)} t2
                    using (product, year, reporter, partner)
            )
            select
                product,
                year,
                reporter,
                partner,
                app_rate,
                eu_rep,
                t2.eu_part,
                eu_rep_rate
            from all_tariffs t1
            left join {self._fq(self.eu_reporter_rates_vn)} t2
                using (product, year, reporter, partner)
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_eu_partner_rates_view(self):
        """
            Calculates the average EU partner rate

            Used by (product_tariffs)
        """
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_partner_rates_vn)} as (
            select * from (
                select
                    product,
                    year,
                    reporter,
                    eu_part,
                    avg(app_rate) as eu_part_rate
                from {self._fq(self.tariffs_with_eu_reporter_rates_vn)}
                where eu_part = 'EUN'
                group by product, year, reporter, eu_part
            ) sq1 where eu_part_rate is not null
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_tariffs_with_world_partner_rates_view(self):
        """
            Required tariffs with expanded world partner rates

            Used by (product_tariffs)
        """
        stmt = f"""
        create materialized view if not exists
            {self._fq(self.tariffs_with_world_partner_rates_vn)} as (
            with world_reported_tariffs as (
                select distinct
                    product,
                    year,
                    reporter as iso3,
                    mfn_rate
                from {self._fq(self.baci_spine_vn)} t1
                left join {self._fq(self.all_tariffs_vn)} t2
                    using (product, year, reporter)
                where rep_req = True and part_iso_num = 0
            )
            select distinct
                product,
                year,
                t1.iso3 as reporter,
                t2.iso3 as partner,
                mfn_rate
            from world_reported_tariffs t1
            cross join {self._fq(self.required_countries_vn)} t2
            where t1.iso3 != t2.iso3
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)
