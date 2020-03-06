from io import BytesIO

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_incremental_data import IncrementalDataPipeline


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

    def process(self, file_info=None, drop_source=True):
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

    _l1_data_column_types = [
        ('product', 'integer'),
        ('reporter', 'integer'),
        ('partner', 'integer'),
        ('year', 'integer'),
        ('assumed_tariff', 'decimal'),
        ('app_rate', 'decimal'),
        ('mfn_rate', 'decimal'),
        ('prf_rate', 'decimal'),
        ('bnd_rate', 'decimal'),
        ('country_average', 'decimal'),
        ('world_average', 'decimal'),
    ]

    def _datafile_to_l0_temp(self, file_info):
        pass

    _l0_l1_data_transformations = {}

    def process(self, file_info=None, drop_source=True):
        self._create_table(self._l1_temp_table, self._l1_column_types, drop_existing=True)
        self._l0_to_l1()

        # swap tables to minimize api downtime
        self.dbi.drop_table(self._l1_table)
        self.dbi.rename_table(
            self.schema,
            self.dbi.parse_fully_qualified(self._l1_temp_table).table,
            self.dbi.parse_fully_qualified(self._l1_table).table,
        )

    def _l0_to_l1(self):
        dit_eu_country_membership = DITEUCountryMembershipPipeline(self.dbi)
        comtrade_country_code_and_iso = ComtradeCountryCodeAndISOPipeline(self.dbi)
        stmt = f"""
        DO
        $do$
        DECLARE
            rec RECORD;
        BEGIN
        FOR rec IN
            SELECT distinct product FROM {self._l0_table}
            where product is not null ORDER BY product
        LOOP
            with eu_countries as (
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
            ), tariffs_and_countries as (
                select
                    t1.reporter,
                    t1.partner,
                    t1.year as year,
                    t1.duty_type as tariff_type,
                    t1.simple_average,
                    t2.tariff_code as reporter_eu,
                    t3.tariff_code as partner_eu
                from {self._l0_table} t1
                left join eu_countries t2
                    on t1.reporter = t2.iso_number and t1.year = t2.year
                left join eu_countries t3
                    on t1.partner = t3.iso_number and t1.year = t3.year
                where t1.product = rec.product
            ), ahs_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'AHS'
            ), prf_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'PRF'
            ), mfn_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'MFN'
            ), bnd_tariffs as (
                select * from tariffs_and_countries where tariff_type = 'BND'
            ), all_tariffs as (
                select
                    reporter,
                    case
                        when partner = 250 then 251
                        when partner = 380 then 381
                        else partner
                    end as partner,
                    year,
                    t1.reporter_eu,
                    t1.partner_eu,
                    t1.simple_average as app_rate,
                    t2.simple_average as prf_rate,
                    t3.simple_average as mfn_rate,
                    t4.simple_average as bnd_rate
                from ahs_tariffs t1
                left join prf_tariffs t2 using (reporter, partner, year)
                left join mfn_tariffs t3 using (reporter, partner, year)
                left join bnd_tariffs t4 using (reporter, partner, year)
                where reporter != partner and reporter != 0 and partner != 0
            ), eu_charging_rates as (
                select reporter, year, 'EUN' as partner_eu, avg(prf_rate) as eu_partner_avg
                from all_tariffs where partner_eu = 'EUN'
                group by reporter, year, partner_eu
            ), eu_member_rates as (
                select partner, year, 'EUN' as reporter_eu, app_rate as eu_reporter_avg
                from all_tariffs where reporter = '918'
            ), tariff_spine as (
                select
                    reporter,
                    partner,
                    year
                from (select distinct reporter from all_tariffs) t1
                cross join (select distinct partner from all_tariffs) cj1
                cross join (select distinct year from all_tariffs) cj2
            ), cleaned_tariffs as (
                select
                    t1.reporter,
                    t1.partner,
                    t1.year,
                    case when t1.partner = '918' then 'EUN' else t3.tariff_code end as reporter_eu,
                    t4.tariff_code as partner_eu,
                    app_rate,
                    prf_rate,
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
                from tariff_spine t1
                left join all_tariffs t2 using (reporter, partner, year)
                left join eu_countries t3 on t1.reporter = t3.iso_number and t1.year = t3.year
                left join eu_countries t4 on t1.partner = t4.iso_number and t1.year = t4.year
                left join eu_charging_rates t5
                    on t1.reporter = t5.reporter and t1.year = t5.year
                        and t5.partner_eu = t4.tariff_code
                left join eu_member_rates t6
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
                        sum(case when final_tariff is null then 0 else 1 end)
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
                            sum(case when filled_tariff_tmp is null then 0 else 1 end)
                                over (partition by reporter, partner ORDER BY year)
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
                prf_rate,
                bnd_rate,
                country_average,
                world_average
            )
            select
                rec.product as product,
                reporter,
                partner,
                year,
                avg(assumed_tariff) as assumed_tariff,
                avg(app_rate) as app_rate,
                avg(mfn_rate) as mfn_rate,
                avg(prf_rate) as prf_rate,
                avg(bnd_rate) as bnd_rate,
                avg(country_average) as country_average,
                avg(world_average) as world_average
            from assumed_tariffs t1
            where year > 2012
            group by rec.product, reporter, partner, year
            order by rec.product, reporter, partner, year;
        END LOOP;
        END
        $do$;
        """
        self.dbi.execute_statement(stmt, raise_if_fail=True)
