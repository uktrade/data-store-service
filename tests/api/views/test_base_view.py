import pytest

from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import ONSPostcodeDirectoryL1


class FakeListView(PipelinePaginatedListView):
    model = ONSPostcodeDirectoryL1


def test_pipeline_columns_required(app_with_db):
    with app_with_db.test_request_context():
        with pytest.raises(NotImplementedError):
            FakeListView().dispatch_request()
