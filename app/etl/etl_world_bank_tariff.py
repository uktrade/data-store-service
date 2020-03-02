from io import BytesIO

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_incremental_data import IncrementalDataPipeline


class WorldBankTariffPipeline(IncrementalDataPipeline):
    organisation = 'world_bank'
    dataset = 'tariff'

    _l0_data_column_types = [
        ('selected_nomen', 'text'),
        ('native_nomen', 'text'),
        ('reporter', 'integer'),
        ('reporter_name', 'text'),
        ('product', 'integer'),
        ('product_name', 'text'),
        ('partner', 'integer'),
        ('partner_name', 'text'),
        ('tariff_year', 'integer'),
        ('trade_year', 'integer'),
        ('trade_source', 'text'),
        ('duty_type', 'text'),
        ('simple_average', 'decimal'),
        ('weighted_average', 'decimal'),
        ('standard_deviation', 'decimal'),
        ('minimum_rate', 'decimal'),
        ('maximum_rate', 'decimal'),
        ('number_of_total_lines', 'integer'),
        ('number_of_domestic_peaks', 'integer'),
        ('number_of_international_peaks', 'integer'),
        ('imports_value_in_1000_usd', 'decimal'),
        ('binding_coverage', 'integer'),
        ('simple_tariff_line_average', 'decimal'),
        ('variance', 'decimal'),
        ('sum_of_rates', 'decimal'),
        ('sum_of_savg_rates', 'decimal'),
        ('count_of_savg_rates_cases', 'integer'),
        ('sum_of_squared_rates', 'decimal'),
        ('number_of_ave_lines', 'integer'),
        ('number_of_na_lines', 'integer'),
        ('number_of_free_lines', 'integer'),
        ('number_of_dutiable_lines', 'integer'),
        ('number_lines_0_to_5', 'integer'),
        ('number_lines_5_to_10', 'integer'),
        ('number_lines_10_to_20', 'integer'),
        ('number_lines_20_to_50', 'integer'),
        ('number_lines_50_to_100', 'integer'),
        ('number_lines_more_than_100', 'integer'),
        ('sum_rate_by_weight_trade_value', 'decimal'),
        ('sum_weight_trade_value_not_null', 'decimal'),
        ('free_imports_in_1000_usd', 'decimal'),
        ('dutiable_imports_in_1000_usd', 'decimal'),
        ('specific_duty_imports_in_1000_usd', 'decimal'),
    ]

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
        dit_eu_country_membership = DITEUCountryMembershipPipeline(self.dbi)
        comtrade_country_code_and_iso = ComtradeCountryCodeAndISOPipeline(self.dbi)
        stmt = f"""
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
                    t1.tariff_year as year,
                    t1.product,
                    t1.trade_source as source,
                    t1.duty_type as tariff_type,
                    t1.simple_average,
                    t1.weighted_average,
                    t2.tariff_code as reporter_eu,
                    t3.tariff_code as partner_eu
                from {self._l0_temp_table} t1
                left join eu_countries t2
                    on t1.reporter = t2.iso_number and t1.tariff_year = t2.year
                left join eu_countries t3
                    on t1.partner = t3.iso_number and t1.tariff_year = t3.year
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
                    t1.product,
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
                left join prf_tariffs t2 using (reporter, partner, year, product, source)
                left join mfn_tariffs t3 using (reporter, partner, year, product, source)
                left join bnd_tariffs t4 using (reporter, partner, year, product, source)
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
                    product,
                    reporter,
                    partner,
                    year
                from (select distinct reporter from all_tariffs) t1
                cross join (select distinct partner from all_tariffs) cj1
                cross join (select distinct year from all_tariffs) cj2
                cross join (select distinct product from all_tariffs) cj3
            ), cleaned_tariffs as (
                select
                    t1.product,
                    t1.reporter,
                    t1.partner,
                    t1.year,
                    case when t1.partner = '918' then 'EUN' else t3.tariff_code end as reporter_eu,
                    t4.tariff_code as partner_eu,
                    app_rate,
                    prf_rate,
                    mfn_rate,
                    bnd_rate,
                    avg(app_rate) OVER (PARTITION BY t1.product) as world_average,
                    avg(app_rate) OVER (PARTITION BY t1.product, t1.reporter) as country_average,
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
                left join all_tariffs t2 using (product, reporter, partner, year)
                left join eu_countries t3 on t1.reporter = t3.iso_number and t1.year = t3.year
                left join eu_countries t4 on t1.partner = t4.iso_number and t1.year = t4.year
                left join eu_charging_rates t5
                    on t1.reporter = t5.reporter and t1.year = t5.year
                        and t5.partner_eu = t4.tariff_code
                left join eu_member_rates t6
                    on t1.partner = t6.partner and t1.year = t6.year
                        and t6.reporter_eu = t3.tariff_code
                where t1.product is not null and t1.partner != t1.reporter
            ), filled_tariffs as (
                select
                    *, (app_rate is null and final_tariff is not null) as eu_derived,
                    coalesce(first_value(final_tariff)
                        OVER (
                            PARTITION BY product, reporter, partner,
                            final_tariff_partition ORDER BY year
                        )
                    , mfn_rate)
                    as filled_tariff_tmp
                from (
                    select
                        *,
                        sum(case when final_tariff is null then 0 else 1 end)
                            over (partition by product, reporter, partner ORDER BY year)
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
                                PARTITION BY product, reporter, partner, filled_tariff_partition
                                ORDER BY year
                            )
                        as filled_tariff
                    from (
                        select
                            *,
                            sum(case when filled_tariff_tmp is null then 0 else 1 end)
                                over (partition by product, reporter, partner ORDER BY year)
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
                product,
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
            group by product, reporter, partner, year
            order by product, reporter, partner, year
        """
        self.dbi.execute_statement(stmt)
