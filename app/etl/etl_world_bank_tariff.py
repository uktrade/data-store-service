import time
from io import BytesIO
from multiprocessing.pool import ThreadPool as Pool
from flask import current_app as flask_app
import psycopg2

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_incremental_data import IncrementalDataPipeline
from app.etl.etl_world_bank_bound_rates import WorldBankBoundRatesPipeline


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r  %2.2f min' % (method.__name__, (te - ts) / 60))
        return result

    return timed


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
        self._create_sequence(self._l0_sequence, drop_existing=self.force)
        self._create_table(self._l0_table, self._l0_column_types, drop_existing=self.force)
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
        ('reporter', 'integer'),
        ('partner', 'integer'),
        ('year', 'integer'),
        ('assumed_tariff', 'decimal'),
        ('app_rate', 'decimal'),
        ('mfn_rate', 'decimal'),
        ('bnd_rate', 'decimal'),
        ('country_average', 'decimal'),
        ('world_average', 'decimal'),
    ]

    def _datafile_to_l0_temp(self, file_info):
        pass

    _l0_l1_data_transformations = {}

    def process(self, file_info=None, drop_source=True, **kwargs):
        drop_existing = not self.continue_transform
        self._create_table(self._l1_temp_table, self._l1_column_types, drop_existing=drop_existing)
        self._l0_to_l1()
        self.finish_processing()

    def finish_processing(self):
        # swap tables to minimize api downtime
        self.dbi.drop_table(self._l1_table)
        self.dbi.rename_table(
            self.schema,
            self.dbi.parse_fully_qualified(self._l1_temp_table).table,
            self.dbi.parse_fully_qualified(self._l1_table).table,
        )

    eu_countries_vn = 'eu_countries'
    all_tariffs_vn = 'all_tariffs'
    eu_charging_rates_vn = 'eu_charging_rates'
    eu_member_rates_vn = 'eu_member_rates'
    tariff_spine_vn = 'tariff_spine'
    cutoff_year = 2012

    @timeit
    def _l0_to_l1(self):
        views = [
            (self.eu_countries_vn, self._create_eu_countries_view),
            (self.all_tariffs_vn, self._create_all_tariffs_view),
            (self.eu_charging_rates_vn, self._create_eu_charging_rates_view),
            (self.eu_member_rates_vn, self._create_eu_member_rates_view),
            (self.tariff_spine_vn, self._create_tariff_spine_view),
        ]
        for view_name, create_view in views:
            fq_view_name = self._fq(view_name)
            if self.force:
                self.dbi.drop_materialized_view(fq_view_name)
            if not self.dbi.table_exists(self.schema, view_name, materialized_view=True):
                create_view()
            else:
                self.dbi.refresh_materialised_view(fq_view_name)
        products = self._get_products()
        p = Pool(processes=5)
        connection_str = str(flask_app.db.engine.url)
        for i, product in enumerate(products):
            print(f'Processing product: {i+1}/{len(products)}', end='\t')
            code = str(product[0])
            p.apply_async(self._clean_and_transform_tariffs, (code, connection_str))
        p.close()
        p.join()
        print('Done!')

    def _fq(self, table_name):
        return self.dbi.to_fully_qualified(table_name, self.schema)

    def _get_products(self):
        where = ""
        if self.continue_transform:
            where = f"where product not in (select distinct product from {self._l1_temp_table})"

        stmt = f"""
        select distinct 
            product 
        from {self._l0_table}
        {where}
        order by product
        """
        return self.dbi.execute_query(stmt, raise_if_fail=True)

    @timeit
    def _clean_and_transform_tariffs(self, product, connection_str):
        conn, curs = None, None
        try:
            stmt = f"""
            with all_product_tariffs as (
                select
                    * 
                from {self._fq(self.all_tariffs_vn)}
                where product = '{product}'
            ), cleaned_tariffs as (
                select
                    t1.reporter,
                    case
                        when t1.partner = 250 then 251
                        when t1.partner = 380 then 381
                        else t1.partner
                    end as partner,
                    t1.year,
                    case when t1.partner = '918' then 'EUN' else t3.tariff_code end as reporter_eu,
                    t4.tariff_code as partner_eu,
                    app_rate,
                    mfn_rate,
                    bnd_rate,
                    avg(app_rate) OVER () as world_average,
                    avg(app_rate) OVER (PARTITION BY t1.reporter) as country_average,
                    coalesce(
                        app_rate,
                        t6.eu_reporter_avg,
                        t5.eu_partner_avg,
                        case
                            when t2.partner_eu = 'EUN' and t2.reporter_eu = 'EUN' then 0
                            else null
                        end
                    ) as final_tariff
                from {self._fq(self.tariff_spine_vn)} t1
                left join all_product_tariffs t2 using (reporter, partner, year)
                left join {self._fq(self.eu_countries_vn)} t3 on t1.reporter = t3.iso_number and t1.year = t3.year
                left join {self._fq(self.eu_countries_vn)} t4 on t1.partner = t4.iso_number and t1.year = t4.year
                left join {self._fq(self.eu_charging_rates_vn)} t5
                    on t1.reporter = t5.reporter and t1.year = t5.year
                        and t5.partner_eu = t4.tariff_code
                left join {self._fq(self.eu_member_rates_vn)} t6
                    on t1.partner = t6.partner and t1.year = t6.year
                        and t6.reporter_eu = t3.tariff_code
                where t1.partner != t1.reporter
            ), filled_tariffs as (
                select
                    *, (app_rate is null and final_tariff is not null) as eu_derived,
                    coalesce(first_value(final_tariff)
                        OVER (
                            PARTITION BY reporter, partner,
                            final_tariff_partition ORDER BY year
                        )
                    , mfn_rate)
                    as filled_tariff_tmp
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
                from (
                    select
                        *,
                        first_value(filled_tariff_tmp)
                            OVER (
                                PARTITION BY reporter, partner, filled_tariff_partition
                                ORDER BY year
                            )
                        as filled_tariff
                    from (
                        select
                            *,
                            count(filled_tariff_tmp) over (partition by reporter, partner ORDER BY year)
                            as filled_tariff_partition
                        from filled_tariffs
                    ) sq1
                ) sq2
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
                country_average,
                world_average
            )
            select
                {product} as product,
                reporter,
                partner,
                year,
                avg(assumed_tariff) as assumed_tariff,
                avg(app_rate) as app_rate,
                avg(mfn_rate) as mfn_rate,
                avg(bnd_rate) as bnd_rate,
                avg(country_average) as country_average,
                avg(world_average) as world_average
            from assumed_tariffs t1
            where year > {self.cutoff_year}
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
    def _create_eu_countries_view(self):
        dit_eu_country_membership = DITEUCountryMembershipPipeline(self.dbi)
        comtrade_country_code_and_iso = ComtradeCountryCodeAndISOPipeline(self.dbi)
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_countries_vn)} as (
            select
                t1.year,
                t1.iso3,
                t1.tariff_code,
                t2.cty_name_english as country,
                t2.cty_code as iso_number
            from {dit_eu_country_membership._l1_table} t1
            left join {comtrade_country_code_and_iso._l1_table} t2
                on t1.iso3 = t2.iso3_digit_alpha
            where tariff_code = 'EUN'
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_all_tariffs_view(self):
        world_bank_bound_rates = WorldBankBoundRatesPipeline(self.dbi)
        stmt = f"""
        create materialized view if not exists {self._fq(self.all_tariffs_vn)} as (
            with tariffs_and_countries as (
                select
                    t1.product,
                    t1.reporter,
                    t1.partner,
                    t1.year as year,
                    t1.duty_type as tariff_type,
                    t1.simple_average,
                    t2.tariff_code as reporter_eu,
                    t3.tariff_code as partner_eu
                from {self._l0_table} t1
                left join {self._fq(self.eu_countries_vn)} t2
                    on t1.reporter = t2.iso_number and t1.year = t2.year
                left join {self._fq(self.eu_countries_vn)} t3
                    on t1.partner = t3.iso_number and t1.year = t3.year
            ), ahs_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'AHS'
            ), mfn_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'MFN'
            ), bnd_tariffs as (
                select distinct reporter, product, bound_rate from {world_bank_bound_rates._l1_table}
            )
            select
                product,
                reporter,
                partner,
                year,
                t1.reporter_eu,
                t1.partner_eu,
                t1.simple_average as app_rate,
                t3.simple_average as mfn_rate,
                t4.bound_rate as bnd_rate
            from ahs_tariffs t1
            left join mfn_tariffs t3 using (product, reporter, partner, year)
            left join bnd_tariffs t4 using (product, reporter)
            where reporter != partner and reporter != 0 and partner != 0
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_eu_charging_rates_view(self):
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_charging_rates_vn)} as (
            select 
                reporter, 
                year, 
                'EUN' as partner_eu, 
                avg(app_rate) as eu_partner_avg
            from {self._fq(self.all_tariffs_vn)} 
            where partner_eu = 'EUN'
            group by reporter, year, partner_eu
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_eu_member_rates_view(self):
        stmt = f"""
        create materialized view if not exists {self._fq(self.eu_member_rates_vn)} as (
            select 
                partner, 
                year, 
                'EUN' as reporter_eu, 
                app_rate as eu_reporter_avg
            from {self._fq(self.all_tariffs_vn)} where reporter = '918'
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)

    @timeit
    def _create_tariff_spine_view(self):
        stmt = f"""
        create materialized view if not exists {self._fq(self.tariff_spine_vn)} as (
            select
                reporter,
                partner,
                year
            from (select distinct reporter from {self._fq(self.all_tariffs_vn)}) t1
            cross join (select distinct partner from {self._fq(self.all_tariffs_vn)}) cj1
            cross join (select distinct year from {self._fq(self.all_tariffs_vn)}) cj2
        )
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)


class WorldBankTariffTestPipeline(WorldBankTariffPipeline):
    organisation = 'world_bank'
    dataset = 'test'


class WorldBankTariffTransformTestPipeline(WorldBankTariffTransformPipeline):
    organisation = WorldBankTariffTestPipeline.organisation
    dataset = WorldBankTariffTestPipeline.dataset


class WorldBankTariffBulkPipeline(WorldBankTariffPipeline):
    organisation = 'world_bank'
    dataset = 'bulk'


class WorldBankTariffTransformBulkPipeline(WorldBankTariffTransformPipeline):
    organisation = WorldBankTariffBulkPipeline.organisation
    dataset = WorldBankTariffBulkPipeline.dataset
