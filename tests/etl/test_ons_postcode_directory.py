from datatools.io.fileinfo import FileInfo

from app.etl.organisation.ons import ONSPostcodeDirectoryPipeline
from tests.utils import rows_equal_table

snapshot1 = 'tests/fixtures/ons/postcode_directory/ONSPD_MAY_2019_UK.csv'
snapshot2 = 'tests/fixtures/ons/postcode_directory/ONSPD_JUL_2019_UK.csv'


class TestONSPostcodeDirectoryPipeline:
    def test_one_datafile(self, app_with_db):
        pipeline = ONSPostcodeDirectoryPipeline(app_with_db.dbi, force=False)
        fi = FileInfo.from_path(snapshot1)
        pipeline.process(fi)

        # check L0
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '198001',
                None,
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '198001',
                '199606',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '1980-01-01',
                None,
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '1980-01-01',
                '1996-06-01',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)

    def test_same_data(self, app_with_db):
        pipeline = ONSPostcodeDirectoryPipeline(app_with_db.dbi, force=False)
        fi = FileInfo.from_path(snapshot1)
        pipeline.process(fi)
        fi.data.seek(0)
        fi2 = FileInfo(fi.name + '2', fi.data)
        pipeline.process(fi2)

        # check L0
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '198001',
                None,
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '198001',
                '199606',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '1980-01-01',
                None,
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '1980-01-01',
                '1996-06-01',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)

    def test_new_data(self, app_with_db):
        pipeline = ONSPostcodeDirectoryPipeline(app_with_db.dbi, force=False)
        fi = FileInfo.from_path(snapshot1)
        pipeline.process(fi, delete_previous=True)
        fi2 = FileInfo.from_path(snapshot2)
        pipeline.process(fi2, delete_previous=True)

        # check L0
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '200001',
                '199606',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '198001',
                '199606',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AD',
                'AB1  0AD',
                'AB1 0AD',
                '198001',
                '199606',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385053',
                '0801092',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090399',
                'S01006514',
                'S02001237',
                'S34003015',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '6A1',
                '57.100556',
                '-2.248342',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l0_table, pipeline)

        # check L1
        expected_rows = [
            (
                'AB1 0AA',
                'AB1  0AA',
                'AB1 0AA',
                '2000-01-01',
                '1996-06-01',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385386',
                '0801193',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.101474',
                '-2.242851',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AB',
                'AB1  0AB',
                'AB1 0AB',
                '1980-01-01',
                '1996-06-01',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385177',
                '0801314',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001270',
                '01C31',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '4B3',
                'S00090303',
                'S01006514',
                'S02001237',
                'S34002990',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '1C3',
                '57.102554',
                '-2.246308',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
            (
                'AB1 0AD',
                'AB1  0AD',
                'AB1 0AD',
                '1980-01-01',
                '1996-06-01',
                'S99999999',
                'S99999999',
                'S12000033',
                'S13002843',
                'S99999999',
                '0',
                '385053',
                '0801092',
                '1',
                'S08000020',
                'S99999999',
                'S92000003',
                'S99999999',
                '0',
                'S14000002',
                'S15000001',
                'S09000001',
                'S22000047',
                'S03000012',
                'S31000935',
                '99ZZ00',
                'S00001364',
                '01C30',
                'S99999999',
                'S01000011',
                'S02000007',
                '6',
                '3C2',
                'S00090399',
                'S01006514',
                'S02001237',
                'S34003015',
                'S03000012',
                'S99999999',
                'S99999999',
                '3',
                '6A1',
                '57.100556',
                '-2.248342',
                'S99999999',
                'S99999999',
                'S23000009',
                '6808',
                'S99999999',
                'S99999999',
            ),
        ]
        assert rows_equal_table(app_with_db.dbi, expected_rows, pipeline._l1_table, pipeline)
