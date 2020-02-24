from io import BytesIO

from app.etl.etl_incremental_data import IncrementalDataPipeline
from app.etl.transforms.world_bank_tariff import CleanWorldBankTariff


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
        ('app_rate', 'text'),
        ('mfn_rate', 'text'),
        ('prf_rate', 'text'),
        ('bnd_rate', 'text'),
        ('country_average', 'decimal'),
        ('world_average', 'decimal')
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

<<<<<<< HEAD
    def _l0_to_l1(self):
        stmt = CleanWorldBankTariff(
            self._l0_temp_table, self._l1_temp_table
        ).get_sql()
        self.dbi.execute_statement(stmt)
=======
    def _l0_to_l1(self, datafile_name):
        pass
>>>>>>> L1 table definition
