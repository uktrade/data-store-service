import re
from abc import ABCMeta, abstractmethod
from collections import namedtuple

from flask import current_app as flask_app
from sqlalchemy import text


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        if obj:
            return self.f(obj)
        return self.f(owner)


class DataPipeline(metaclass=ABCMeta):
    """Abstract class for Data Pipelines.

    The main entry point and usage of this class is the
    process method. Everything else can be overridden and
    adapted to suit that particular clean pipeline.

    The two properties which are essential are organisation and
    dataset which should uniquely identify the clean pipeline. Though
    "format" of datafiles are often used, they are internal to the
    clean pipelines and do not need to be exposed.

    The LN tables are considered now to be internal to the pipeline
    and can be managed in a way that is appropriate for that
    pipeline. For example, they can be destroyed and recreated for
    each datafile, or perhaps appended to and reused, or perhaps there
    are separate L tables for each datafile differentiated by a hash
    of the datafile name.


    """

    def set_option_defaults(self, options):
        options.setdefault('force', False)
        options.setdefault('delete_previous', False)
        return options

    def get_options(self, options):
        options = self.set_option_defaults(options)
        return namedtuple('Options', options.keys())(*options.values())

    def __init__(self, dbi, **kwargs):
        self.dbi = dbi
        self.options = self.get_options(kwargs)
        if not dbi:
            flask_app.logger.error(
                f'warning: dbi ({dbi}) is not valid; '
                'this pipeline instance cannot process datafiles or create schemas'
            )
        else:
            self.dbi.execute_statement(
                text("SET statement_timeout TO '20h' ")
            )  # If a query takes longer over 20hours, stop it!
            if self.organisation and self.dataset:
                self._create_schema_if_not_exists(self.schema)

    def __str__(self):
        return f'<{self.__class__.__name__}>'

    def __getattr__(self, name):
        pat = re.compile('_l([0-9])_table')
        match = pat.match(name)
        if match:
            num = match.group(1)
            return self._fully_qualified(f'L{num}')
        raise AttributeError(f'no property named {name}')

    @classproperty
    def id(cls):
        return (
            f"{cls.organisation}.{cls.dataset}"
            f"{('.' + cls.subdataset) if cls.subdataset else ''}"
            f"{('.' + cls.format_version) if cls.format_version else ''}"
        )

    @classproperty
    def data_source(cls):
        return f'{cls.organisation}.{cls.dataset}'

    @classproperty
    @abstractmethod
    def dataset(cls):
        """String [a-zA-Z0-9._-]+ (no quotation marks)"""

    @classproperty
    @abstractmethod
    def organisation(cls):
        """String [a-zA-Z0-9._-]+ (no quotation marks)"""

    subdataset, format_version = None, None

    @abstractmethod
    def process(self, fileinfo, **kwargs):
        """Takes a datatools.io.fileinfo.FileInfo object"""
        ...

    @classproperty
    def schema(self):
        return f'{self.organisation}.{self.dataset}'

    def _create_schema_if_not_exists(self, schema):
        if not self.dbi.schema_exists(schema):
            self.dbi.create_schema(schema)

    def _fully_qualified(self, table):
        if table[0] == table[-1] == '"':
            return table
        return f'"{self.schema}"."{table}"'


class LDataPipeline(DataPipeline, metaclass=ABCMeta):

    L0_TABLE = 'L0'
    L1_TABLE = 'L1'

    @property
    def l1_helper_columns(self):
        return [
            ('id', 'serial primary key'),  # primary key
            ('data_source_row_id', 'int'),  # reference to L0 id column
        ]
