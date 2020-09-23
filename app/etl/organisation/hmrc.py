from io import BytesIO, StringIO
from zipfile import ZipFile

from app.etl.pipeline_type.incremental_data import L1IncrementalDataPipeline


class HMRCBasePipeline(L1IncrementalDataPipeline):
    organisation = 'hmrc'

    _l0_data_column_types = [
        ('month', 'text'),
        ('row_type', 'text'),
        ('company_name', 'text'),
        ('address1', 'text'),
        ('address2', 'text'),
        ('address3', 'text'),
        ('address4', 'text'),
        ('address5', 'text'),
        ('postcode', 'text'),
        ('export_item_codes', 'text'),
    ]

    _l1_data_column_types = [
        ('datetime', 'timestamp'),
        ('company_name', 'text'),
        ('address', 'text'),
        ('postcode', 'text'),
        ('export_item_codes', 'text[]'),
    ]

    _l0_l1_data_transformations = {
        'datetime': "to_timestamp(month, 'YYYYMM')",
        'company_name': 'lower(trim(company_name))',
        'address': (
            """nullif(lower(trim(concat_ws(' ', """
            """address1, address2, address3, address4, address5))), '')"""
        ),
        'postcode': "upper(replace(postcode, ' ', ''))",
        'export_item_codes': "string_to_array(export_item_codes, ',', '')",
    }

    def _datafile_to_l0_temp(self, file_info):
        subprocess = DatafileToL0Process()
        subprocess.process(
            file_info, self.dbi, self._l0_temp_table, [c for c, _ in self._l0_data_column_types]
        )


class DatafileToL0Process:
    def process(self, datafile, dbi, table, columns):
        self.dbi = dbi
        csv_data = self._get_csv_data(datafile.data)
        self._csv_data_to_l0(csv_data, table, columns)

    def _get_lines_from_file_data(self, file_data):
        # we expect the data to represent a zipfile of zipfiles
        def gen_data(zipfile):
            for n in zipfile.namelist():
                yield zipfile.read(n), n

        zf = ZipFile(file_data)
        for sub_zf_data, n in gen_data(zf):
            csv_data = (
                next(gen_data(ZipFile(BytesIO(sub_zf_data))))[0]
                if n.endswith('.zip')
                else sub_zf_data
            )
            yield from csv_data.decode('mac-roman').splitlines()

    def _get_csv_data(self, file_data):
        rv = StringIO()
        for line in self._get_lines_from_file_data(file_data):
            formatted_line = self._format_monthly_exporter_file_line(line)
            if formatted_line is not None:
                rv.write(formatted_line + '\n')
        rv.seek(0)
        return rv

    def _csv_data_to_l0(self, csv_data, table, columns):
        self.dbi.dsv_buffer_to_table(
            csv_data, table, has_header=False, null='', sep=',', columns=columns, quote='"'
        )

    def _format_monthly_exporter_file_line(self, line):
        values = line.split('\t')
        if len(values) < 10:
            return None

        def _format_str(string):
            return '"' + string.replace('"', '') + '"'

        month = _format_str(values[0])
        row_type = _format_str(values[1])
        company_name = _format_str(values[2])
        address_line1 = _format_str(values[3])
        address_line2 = _format_str(values[4])
        address_line3 = _format_str(values[5])
        address_line4 = _format_str(values[6])
        address_line5 = _format_str(values[7])
        postcode = _format_str(values[8])
        goods_codes = '"' + ','.join(v.replace('\n', '') for v in values[9:] if v != '') + '"'
        return ','.join(
            [
                month,
                row_type,
                company_name,
                address_line1,
                address_line2,
                address_line3,
                address_line4,
                address_line5,
                postcode,
                goods_codes,
            ]
        )


class HMRCExportersPipeline(HMRCBasePipeline):
    dataset = 'exporters'

    data_source_name = 'hmrc.exporters'
    event_description = 'Export of goods outside the EU'
