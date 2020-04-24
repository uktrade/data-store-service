from app.etl.base import LDataPipeline


class TestClassMembers:
    def test(self):
        assert LDataPipeline.L0_TABLE == 'L0'
        assert LDataPipeline.L1_TABLE == 'L1'
        assert LDataPipeline.L2_TABLE == 'L2'
