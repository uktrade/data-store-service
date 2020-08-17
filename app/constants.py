from enum import Enum

DEFAULT_CSV_DELIMITER = ','
DEFAULT_CSV_QUOTECHAR = '"'
CSV_NULL_VALUES = ['NULL', 'None', 'N/A', "''"]
YES = 'yes'
NO = 'no'


class BaseEnum(Enum):
    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class DatafileState(BaseEnum):
    PROCESSING = 'processing'
    FAILED = 'failed'
    PROCESSED = 'processed'
    IGNORED = 'ignored'


class DataUploaderFileState(BaseEnum):
    UPLOADING = 'uploading'
    UPLOADED = 'uploaded'
    VERIFIED = 'verified'
    PROCESSING_DSS = 'processing_dss'
    PROCESSING_DATAFLOW = 'processing_dataflow'
    COMPLETED = 'completed'
    FAILED = 'failed'


class DataUploaderDataTypes(BaseEnum):
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    DATE = 'date'
    TIMESTAMP = 'timestamp'
    NUMERIC = 'numeric'
    TEXT = 'text'
