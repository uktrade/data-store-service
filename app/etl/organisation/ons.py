import datetime
import re
import zipfile
from io import BytesIO

from datatools.io.fileinfo import FileInfo

from app.etl.pipeline_type.incremental_data import L1IncrementalDataPipeline


class ONSPostcodeDirectoryPipeline(L1IncrementalDataPipeline):
    organisation = 'ons'
    dataset = 'postcode_directory'

    _l0_data_column_types = [
        ('pcd', 'text'),
        ('pcd2', 'text'),
        ('pcds', 'text'),
        ('dointr', 'text'),
        ('doterm', 'text'),
        ('oscty', 'text'),
        ('ced', 'text'),
        ('oslaua', 'text'),
        ('osward', 'text'),
        ('parish', 'text'),
        ('usertype', 'text'),
        ('oseast1m', 'text'),
        ('osnrth1m', 'text'),
        ('osgrdind', 'text'),
        ('oshlthau', 'text'),
        ('nhser', 'text'),
        ('ctry', 'text'),
        ('rgn', 'text'),
        ('streg', 'text'),
        ('pcon', 'text'),
        ('eer', 'text'),
        ('teclec', 'text'),
        ('ttwa', 'text'),
        ('pct', 'text'),
        ('nuts', 'text'),
        ('statsward', 'text'),
        ('oa01', 'text'),
        ('casward', 'text'),
        ('park', 'text'),
        ('lsoa01', 'text'),
        ('msoa01', 'text'),
        ('ur01ind', 'text'),
        ('oac01', 'text'),
        ('oa11', 'text'),
        ('lsoa11', 'text'),
        ('msoa11', 'text'),
        ('wz11', 'text'),
        ('ccg', 'text'),
        ('bua11', 'text'),
        ('buasd11', 'text'),
        ('ru11ind', 'text'),
        ('oac11', 'text'),
        ('lat', 'text'),
        ('long', 'text'),
        ('lep1', 'text'),
        ('lep2', 'text'),
        ('pfa', 'text'),
        ('imd', 'text'),
        ('calncv', 'text'),
        ('stp', 'text'),
    ]

    publication_date = None

    def process(self, file_info, drop_source=True, **kwargs):
        file_regex = r'(.*?)/ONSPD_(?P<date>\w{3}_\d{4})\_UK.csv$'
        zf = zipfile.ZipFile(file_info.data, mode='r')
        members, file_found = zf.namelist(), False
        for file_name in members:
            file_match = re.match(file_regex, file_name)
            if file_match:
                file_found = True
                file = zf.open(file_name)
                datestr = file_match.group('date')
                break
        if not file_found:
            raise ValueError('No valid CSV found in zip file.')
        date = datetime.datetime.strptime(datestr, '%b_%Y').date()
        self.publication_date = datetime.datetime.strftime(date, '%Y-%m-%d')
        csv_file_info = FileInfo(file_info.name, file)
        super().process(csv_file_info, drop_source, **kwargs)

    def _datafile_to_l0_temp(self, file_info):
        csv_data_no_empty_quotes_and_date_appended = BytesIO(
            file_info.data.read().replace(b'""', b'')
        )
        self.dbi.dsv_buffer_to_table(
            csv_buffer=csv_data_no_empty_quotes_and_date_appended,
            fq_table_name=self._l0_temp_table,
            columns=[c for c, _ in self._l0_data_column_types],
            has_header=True,
            sep=',',
            quote='"',
        )

    _l1_data_column_types = (
        _l0_data_column_types[:3]
        + [('dointr', 'date'), ('doterm', 'date')]
        + _l0_data_column_types[5:]
        + [('publication_date', 'date')]
    )

    @property
    def _l0_l1_data_transformations(self):
        return {
            'dointr': "to_date(dointr, 'YYYYMM')",
            'doterm': "to_date(doterm, 'YYYYMM')",
            'publication_date': f"to_date('{self.publication_date}', 'YYYY-MM-DD')",
        }
