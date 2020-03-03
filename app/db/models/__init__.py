import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import DDL, event
from sqlalchemy.orm import load_only
from sqlalchemy.sql import ClauseElement

from app.etl.etl_comtrade_country_code_and_iso import ComtradeCountryCodeAndISOPipeline
from app.etl.etl_dit_eu_country_membership import DITEUCountryMembershipPipeline
from app.etl.etl_dit_reference_postcodes import DITReferencePostcodesPipeline
from app.etl.etl_ons_postcode_directory import ONSPostcodeDirectoryPipeline
from app.etl.etl_world_bank_tariff import WorldBankTariffPipeline

db = SQLAlchemy()
sql_alchemy = db

# aliases
_sa = sql_alchemy
_col = db.Column
_text = db.Text
_int = db.Integer
_dt = db.DateTime
_bigint = _sa.BigInteger
_bool = db.Boolean
_num = db.Numeric
_array = _sa.ARRAY
_date = _sa.Date
_enum = _sa.Enum
_float = _sa.Float
_decimal = _num


class BaseModel(db.Model):
    __abstract__ = True

    def save(self):
        _sa.session.add(self)
        _sa.session.commit()

    @classmethod
    def create_table(cls):
        db.metadata.create_all(bind=db.engine, tables=[cls.__table__], checkfirst=True)

    @classmethod
    def drop_table(cls):
        cls.__table__.drop(db.engine, checkfirst=True)

    @classmethod
    def recreate_table(cls):
        cls.drop_table()
        cls.create_table()

    @classmethod
    def get_schema(cls):
        if 'schema' in cls.__table_args__:
            return cls.__table_args__['schema']
        else:
            return 'public'

    @classmethod
    def get_fq_table_name(cls):
        return f'"{cls.get_schema()}"."{cls.__tablename__}"'

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        """
        Creates a new object or returns existing.

        Example:
            object, created = Model.get_or_create(a=1, b=2, defaults=dict(c=3))

        :param defaults: (dictionary) of fields that should be saved on new instance
        :param kwargs: fields to query for an object
        :return: (Object, boolean) (Object, created)
        """
        instance = _sa.session.query(cls).filter_by(**kwargs).first()
        _sa.session.close()
        if instance:
            return instance, False
        else:
            params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
            params.update(defaults or {})
            instance = cls(**params)
            instance.save()
            return instance, True

    @classmethod
    def get_dataframe(cls, columns=None):
        query = cls.query
        if columns:
            query = query.options(load_only(*columns))
        return pd.read_sql(query.statement, cls.query.session.bind, index_col=cls.id.description)


def create_schemas(*args, **kwargs):
    schemas = [
        'operations',
        'admin',
        ONSPostcodeDirectoryPipeline.schema,
        DITReferencePostcodesPipeline.schema,
        WorldBankTariffPipeline.schema,
        ComtradeCountryCodeAndISOPipeline.schema,
        DITEUCountryMembershipPipeline.schema,
    ]
    for schema in schemas:
        _sa.engine.execute(DDL(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    _sa.session.commit()


event.listen(BaseModel.metadata, 'before_create', create_schemas)
