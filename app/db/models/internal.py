import datetime
import uuid
from collections import defaultdict

from data_engineering.common.db.models import (
    _array,
    _bool,
    _col,
    _dt,
    _enum,
    _foreign_key,
    _int,
    _relationship,
    _sa,
    _text,
    _unique,
    _uuid,
    BaseModel,
)
from slugify import slugify
from sqlalchemy import event, or_

from app import constants
from app.constants import DatafileState


class DatafileRegistryModel(BaseModel):
    __tablename__ = 'datafile_registry'
    __table_args__ = {'schema': 'operations'}

    processing_state = _enum(
        *constants.DatafileState.values(), name='processing_state', inherit_schema=True
    )

    id = _col('id', _int, primary_key=True, autoincrement=True)
    source = _col(_text, nullable=False)
    file_name = _col(_text)
    state = _col(processing_state, nullable=False, default=False)
    error_message = _col(_text)
    created_timestamp = _col(
        'created_timestamp', _dt, nullable=False, default=lambda: datetime.datetime.utcnow()
    )
    updated_timestamp = _col('updated_timestamp', _dt, onupdate=lambda: datetime.datetime.utcnow())

    __mapper_args__ = {'order_by': 'created_timestamp'}

    @classmethod
    def get_update_or_create(cls, source, file_name, state=None, error_message=None):
        if not file_name:
            # always create new row if file_name is empty
            instance = DatafileRegistryModel(
                source=source, file_name=file_name, state=state, error_message=error_message
            )
            instance.save()
            return instance, True

        # update row if source/file_name already exists otherwise create new row
        defaults = {
            'state': state,
            'error_message': error_message,
        }
        clean_datafile, created = DatafileRegistryModel.get_or_create(
            source=source, file_name=file_name, defaults=defaults,
        )
        if not created:
            update_state = state is not None
            update_error_message = error_message is not None
            if update_state:
                clean_datafile.state = state
            if update_error_message:
                clean_datafile.error_message = error_message
            if update_state or update_error_message:
                clean_datafile.save()
        return clean_datafile, created

    @classmethod
    def get_processed_or_ignored_datafiles(cls, data_source=None):
        processed_dfs_per_pipeline = defaultdict(list)
        query = _sa.session.query(cls.source, cls.file_name)
        if data_source:
            query = query.filter(cls.source == data_source)
        query = query.filter(
            or_(
                cls.state == DatafileState.PROCESSED.value, cls.state == DatafileState.IGNORED.value
            )
        )
        for row in query:
            processed_dfs_per_pipeline[row[0]].append(row[1])
        return processed_dfs_per_pipeline


class Pipeline(BaseModel):
    __tablename__ = 'pipeline'
    __table_args__ = (
        _unique('organisation', 'dataset', name='organisation_dataset_unique_together'),
        {'schema': 'public'},
    )

    id = _col('id', _int, primary_key=True, autoincrement=True)
    organisation = _col(_text, nullable=False)
    dataset = _col(_text, nullable=False)
    slug = _col(_text, nullable=False)
    column_types = _col(_array(_text), nullable=False)
    delimiter = _col(_text, nullable=False, server_default=',')
    quote = _col(_text, server_default='"')
    data_files = _relationship('PipelineDataFile', backref='pipeline')

    @staticmethod
    def generate_slug(mapper, connection, target):
        if not target.slug:
            target.slug = slugify(f'{target.organisation} {target.dataset}')

    @property
    def pipeline_schema(self):
        return f'{self.organisation}.{self.dataset}'

    def __str__(self):
        return self.pipeline_schema


event.listen(Pipeline, 'before_insert', Pipeline.generate_slug)


class PipelineDataFile(BaseModel):
    __tablename__ = 'pipeline_data_file'

    id = _col(_uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data_file_url = _col(_text, nullable=False)
    pipeline_id = _col(_int, _foreign_key('public.pipeline.id'), nullable=False)
    deleted = _col(_bool, default=False)
    uploaded_at = _col(_dt, default=lambda: datetime.datetime.utcnow())
    processed_at = _col(_dt)
