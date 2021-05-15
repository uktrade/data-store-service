import csv
import io
from abc import abstractmethod

from app.etl.pipeline_type.base import classproperty, DataPipeline


class RebuildSchemaPipeline(DataPipeline):

    csv_to_model_mapping = None
    null_values = ['null', 'NULL', '']

    def process(self, file_info):
        data = file_info.data.read()
        data_text = io.TextIOWrapper(io.BytesIO(data))
        csv_reader = csv.reader(data_text, delimiter=',')
        headers = next(csv_reader)
        if self.csv_to_model_mapping is not None:
            headers = [self.csv_to_model_mapping[header] for header in headers]
        session = self.dbi.db.create_scoped_session()
        objects = []
        for row in csv_reader:
            kwargs = {}
            for field, value in zip(headers, row):
                kwargs[field] = None if value in self.null_values else value
            obj = self.sql_alchemy_model(**kwargs)
            objects.append(obj)
        session.bulk_save_objects(objects)
        session.commit()

    @classproperty
    @abstractmethod
    def sql_alchemy_model(cls):
        '''data model for this table'''
        ...
