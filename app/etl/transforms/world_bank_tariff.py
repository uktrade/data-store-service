
class CleanWorldBankTariff:

    def __init__(self, l0_db_model, l1_db_model, start_year=1998, end_year=2019):
        self.years = [i for i in range(start_year, end_year)]
        self.l0_db_model = l0_db_model
        self.l1_db_model = l1_db_model

    def year_fill(self):
        sql = []
        for year in self.years:
            sql .append(f"""
            select max(tariff_year) as max_tariff,
                    product,
                    partner,
                    reporter,
                    {year} as year
                  from {self.l0_db_model}
             where product is not null
             and partner != reporter
             group by product, partner, reporter, year
             """)
        return sql

    def get_partner_reporter_combinations_for_each_year(self, query_name):
        return f"""
        temp_year_table as (
             {'UNION ALL'.join(self.year_fill())}
        ),
        
        {query_name} as (
        select *,
        0 as assumed_tariff
        from temp_year_table 
        where year >= max_tariff
        )
        """

    def get_rate_averages(self, query_name, query_from):
        return f"""
        {query_name} as (
            select distinct
               p.*,
               case 
                 when bnd.simple_average is not null then bnd.simple_average::text else 'NA'::text
               end as bnd_rate,
               case 
                 when prf.simple_average is not null then prf.simple_average::text else 'NA'::text
               end as prf_rate,
               case 
                 when ash.simple_average is not null then ash.simple_average::text else 'NA'::text
               end as app_rate,
               case 
                 when mfn.simple_average is not null then mfn.simple_average::text else 'NA'::text
               end as mfn_rate
              from {query_from} p 
            left join {self.l0_db_model} bnd on p.partner = bnd.partner and 
                 p.reporter = bnd.reporter and bnd.duty_type = 'BND' and p.year::text = bnd.tariff_year::text
            left join {self.l0_db_model} prf on p.partner = prf.partner and 
                p.reporter = prf.reporter and prf.duty_type = 'PRF' and p.year::text = prf.tariff_year::text
            left join {self.l0_db_model} mfn on p.partner = mfn.partner and 
                p.reporter = mfn.reporter and mfn.duty_type = 'MFN' and p.year::text = mfn.tariff_year::text
            left join {self.l0_db_model} ash on p.partner = ash.partner and 
                p.reporter = ash.reporter and ash.duty_type = 'AHS' and p.year::text = ash.tariff_year::text
        )
        """

    def get_country_average(self, query_name, query_from):
        return f"""
        {query_name} as (
            select
            p.reporter,
            AVG (p.app_rate::float) as country_average
            from {query_from} p
            where
            p.app_rate != 'NA'
            group
            by
            p.reporter
        )
        """

    def get_world_average(self, query_name, query_from):
        return f"""
        {query_name} as (
            select
            p.*,
            s.country_average as country_average,
            (select AVG(app_rate::float) from {query_from} where app_rate != 'NA') 
            as world_average
            from {query_from} p
            left join country_average s
            on s.reporter = p.reporter
        )
        """

    def insert_data_to_L1(self, query_from):
        return f"""
        insert into {self.l1_db_model} (
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
        ) select product,
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
        from {query_from}
        """

    def get_sql(self):
        initial_query_name = 'base_product'
        country_average_table_name = 'country_average'
        intermediate_table_name = 'products_with_rate_averages'
        final_table = 'populated_products'

        sql = f"""
        with {self.get_partner_reporter_combinations_for_each_year(initial_query_name)},
        {self.get_rate_averages(intermediate_table_name, initial_query_name)},
        {self.get_country_average(country_average_table_name, intermediate_table_name)},
        {self.get_world_average(final_table, intermediate_table_name)}
        {self.insert_data_to_L1(final_table)}

        """
        return sql