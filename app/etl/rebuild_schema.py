import csv
import io
from abc import abstractmethod

from sqlalchemy import DDL

from app.etl.etl_base import classproperty, DataPipeline


class RebuildSchemaPipeline(DataPipeline):

    csv_to_model_mapping = None

    def process(self, file_info):
        data = file_info.data.read()
        data_text = io.TextIOWrapper(io.BytesIO(data))
        csv_reader = csv.reader(data_text, delimiter=',')
        headers = next(csv_reader)
        if self.csv_to_model_mapping is not None:
            headers = [self.csv_to_model_mapping[header] for header in headers]
        session = self.dbi.db.create_scoped_session()
        session.execute(DDL('SET session_replication_role = replica;'))
        try:
            objects = []
            for row in csv_reader:
                kwargs = {field: value for field, value in zip(headers, row)}
                obj = self.sql_alchemy_model(**kwargs)
                objects.append(obj)
            session.bulk_save_objects(objects)
            session.commit()
        finally:
            session.execute(DDL('SET session_replication_role = DEFAULT;'))

    @classproperty
    @abstractmethod
    def sql_alchemy_model(cls):
        ''' data model for this table '''
        ...
