import csv
import io
from abc import ABCMeta, abstractmethod

from flask import current_app

from app.etl.etl_base import classproperty, DataPipeline


class RebuildSchemaPipeline(DataPipeline, metaclass=ABCMeta):
    def __init__(self, dbi, **kwargs):
        self.dbi = dbi
        self.options = self.get_options(kwargs)
        if not dbi:
            current_app.logger.error(
                f'warning: dbi ({dbi}) is not valid; '
                'this pipeline instance cannot process datafiles or create schemas'
            )

    @classproperty
    @abstractmethod
    def sql_alchemy_model(cls):
        ''' data model for this table '''
        ...

    def process(self, file_info):
        data = file_info.data.read()
        data_text = io.TextIOWrapper(io.BytesIO(data))
        csv_reader = csv.reader(data_text, delimiter=',')
        headers = next(csv_reader)
        session = self.dbi.db.create_scoped_session()
        for row in csv_reader:
            kwargs = {field: value for field, value in zip(headers, row)}
            obj = self.sql_alchemy_model(**kwargs)
            session.add(obj)
        session.commit()
