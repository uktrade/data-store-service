from io import BytesIO

from app.etl.snapshot_data import SnapshotDataPipeline


class ONSPostcodeDirectoryPipeline(SnapshotDataPipeline):
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

    _l1_data_column_types = (
        _l0_data_column_types[:3]
        + [('dointr', 'date'), ('doterm', 'date')]
        + _l0_data_column_types[5:]
    )

    _l0_l1_data_transformations = {
        'dointr': "to_date(dointr, 'YYYYMM')",
        'doterm': "to_date(doterm, 'YYYYMM')",
    }
