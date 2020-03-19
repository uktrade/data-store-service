import datetime
from collections import defaultdict

from common.db.models import (
    _col,
    _dt,
    _enum,
    _int,
    _sa,
    _text,
    BaseModel,
)
from sqlalchemy import or_

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
