import pytest
from sqlalchemy.exc import IntegrityError

from tests.fixtures.factories import PipelineFactory


def test_pipeline_create_slug_if_none_provided(app_with_db):
    pipeline = PipelineFactory(organisation='hello', dataset='goodbye')
    assert pipeline.slug == 'hello-goodbye'
    assert pipeline.organisation == 'hello'
    assert pipeline.dataset == 'goodbye'


def test_pipeline_save_slug_if_provided(app_with_db):
    pipeline = PipelineFactory(organisation='hello', dataset='goodbye', slug='goodbye-hello')
    assert pipeline.slug == 'goodbye-hello'


def test_pipeline_organisation_and_dataset_unique_together(app_with_db):
    PipelineFactory(organisation='hello', dataset='goodbye')
    with pytest.raises(IntegrityError):
        PipelineFactory(organisation='hello', dataset='goodbye')
