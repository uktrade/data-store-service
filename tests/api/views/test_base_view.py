import pytest
from data_engineering.common.db.models import HawkUsers

from app.api.views.base import PipelinePaginatedListView
from app.db.models.external import ONSPostcodeDirectoryL1
from tests.api.views import make_hawk_auth_request


class FakeListView(PipelinePaginatedListView):
    model = ONSPostcodeDirectoryL1


def test_pipeline_columns_required(app_with_db):
    with app_with_db.test_request_context():
        with pytest.raises(NotImplementedError):
            FakeListView().dispatch_request()


def test_get_hawk_request_when_no_users_in_db(app_with_db):
    HawkUsers.query.delete()
    client = app_with_db.test_client()
    response = make_hawk_auth_request(client, '/api/v1/get-dit-reference-postcodes/')
    assert response.status_code == 401
