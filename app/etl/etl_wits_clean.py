from app.db.models.external import (
    Product0201,
    Product040110,
    WITSCleanedData,
)

HS_MODEL_MAPPING = {
    '0201': Product0201,
    '040110': Product040110
}


import sqlalchemy.exc
from flask import current_app as flask_app


class ETLTask:
    name = ''

    def __init__(self, sql, model, drop_table=False):
        self.drop_table = drop_table
        self.sql = sql
        self.model = model

    def __call__(self, *args, **kwargs):
        connection = flask_app.db.engine.connect()
        transaction = connection.begin()
        try:
            if self.drop_table is True:
                self.model.drop_table()
            self.model.create_table()
            result = connection.execute(self.sql)
            transaction.commit()
            return {
                'status': 200,
                'rows': result.rowcount,
                'task': self.name,
            }
        except sqlalchemy.exc.ProgrammingError as err:
            transaction.rollback()
            flask_app.logger.error(f'Error running task, "{self.name}". Error: {err}')
            raise err
        finally:
            connection.close()


class CleanWITSProduct:

    def __init__(self, hs_code):
        self.years = [i for i in range(2013, 2019)]
        self.hs_code = hs_code
        self.db_model = HS_MODEL_MAPPING[hs_code]

    def year_fill(self):
        sql = []
        for year in self.years:
            sql .append(f"""
            select distinct
                    product,
                    partner,
                    reporter,
                    {year} as year,
                     0 as assumed_tariff
                  from {self.db_model.get_fq_table_name()}
            """)
        return sql

    def insert_rows_with_empty_rates(self):
        sql = f"""

        with product_combos_1 as (
             {'UNION ALL'.join(self.year_fill())}
        ),
         
        product_combos_2 as (
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
              from product_combos_1 p 
            left join {self.db_model.get_fq_table_name()} bnd on p.partner = bnd.partner and 
                 p.reporter = bnd.reporter and bnd.duty_type = 'BND' and p.year::text = bnd.tariff_year::text
            left join {self.db_model.get_fq_table_name()} prf on p.partner = prf.partner and 
                p.reporter = prf.reporter and prf.duty_type = 'PRF' and p.year::text = prf.tariff_year::text
            left join {self.db_model.get_fq_table_name()} mfn on p.partner = mfn.partner and 
                p.reporter = mfn.reporter and mfn.duty_type = 'MFN' and p.year::text = mfn.tariff_year::text
            left join {self.db_model.get_fq_table_name()} ash on p.partner = ash.partner and 
                p.reporter = ash.reporter and ash.duty_type = 'AHS' and p.year::text = ash.tariff_year::text
        ),
        
        product_combos_3 as (
           select 
           p.reporter as reporter,
           AVG (p.app_rate::float) as country_average
           from product_combos_2 p
           where p.app_rate != 'NA'
           group by p.reporter
        ),
        
        product_combos as (
            select 
            p.*,
            s.country_average as country_average,
            (select AVG(app_rate::float) from product_combos_2 where app_rate != 'NA') as world_average
            from product_combos_2 p
            left join product_combos_3 s on s.reporter = p.reporter
        )
  
        insert into {WITSCleanedData.get_fq_table_name()} (
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
        from product_combos
    """
        obj = ETLTask(
            sql, WITSCleanedData, drop_table=True
        )
        obj()

    def main(self):
        self.insert_rows_with_empty_rates()

