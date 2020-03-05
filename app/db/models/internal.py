import datetime
from collections import defaultdict

from sqlalchemy import or_

from app import constants
from app.constants import DatafileState
from app.db.models import (
    _array,
    _col,
    _dt,
    _enum,
    _int,
    _sa,
    _text,
    BaseModel,
)


class HawkUsers(BaseModel):

    __tablename__ = 'hawk_users'
    __table_args__ = {'schema': 'public'}

    id = _col(_text, primary_key=True)
    key = _col(_text)
    scope = _col(_array(_text))
    description = _col(_text)

    @classmethod
    def get_client_key(cls, client_id):
        query = _sa.session.query(cls.key).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def get_client_scope(cls, client_id):
        query = _sa.session.query(cls.scope).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def add_user(cls, client_id, client_key, client_scope, description):
        cls.get_or_create(
            id=client_id,
            defaults={'key': client_key, 'scope': client_scope, 'description': description},
        )


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
        'created_timestamp', _dt, nullable=False, default=datetime.datetime.utcnow
    )
    updated_timestamp = _col('updated_timestamp', _dt, onupdate=datetime.datetime.utcnow)

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
