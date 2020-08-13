import csv
import os.path
import re
from functools import reduce
from io import BytesIO

import tabulator
import unicodecsv
from flask import current_app as app
from tableschema import Schema
from tabulator import TabulatorException

from app.constants import CSV_NULL_VALUES, DataUploaderDataTypes


class CSVParser:
    @classmethod
    def get_csv_as_utf_8_byte_stream(
        cls, full_url, delimiter=",", quotechar='"', encoding=None,
    ):
        try:
            with tabulator.Stream(
                full_url,
                headers=1,
                ignore_blank_headers=True,
                delimiter=delimiter,
                quotechar=quotechar,
                encoding=encoding,
                format='csv',
                post_parse=[cls.check_and_clean_up_row],
            ) as csv_file:
                file_contents_utf_8 = BytesIO()
                writer = unicodecsv.writer(
                    file_contents_utf_8, encoding='utf-8', delimiter=delimiter, quotechar=quotechar,
                )
                writer.writerow(csv_file.headers)
                for row in csv_file.iter():
                    writer.writerow(row)
            file_contents_utf_8.seek(0)
            return file_contents_utf_8
        except (TabulatorException, csv.Error) as e:
            error_message = str(e)
            app.logger.error(error_message)
            raise e

    @classmethod
    def get_csv_sample(
        cls,
        url,
        delimiter=",",
        quotechar='"',
        number_of_lines_sample=4,
        number_of_lines_infer=100000,
        encoding=None,
    ):
        bucket = app.config['s3']['bucket_url']
        full_url = os.path.join(bucket, url)
        try:
            with tabulator.Stream(
                full_url,
                headers=1,
                sample_size=number_of_lines_sample,
                ignore_blank_headers=True,
                delimiter=delimiter,
                quotechar=quotechar,
                format='csv',
                force_parse=True,
                encoding=encoding,
                post_parse=[cls.check_and_clean_up_row],
            ) as csv_file:
                sample = csv_file.sample
                contents = csv_file.read(limit=number_of_lines_infer)
                headers = csv_file.headers

            if not headers:
                raise csv.Error(
                    'no headers found. The first line of the csv should '
                    'contain the column headers.'
                )
            invalid_headings = list(filter(lambda x: not re.match("^[a-z][a-z0-9_]*$", x), headers))
            if invalid_headings:
                joined_invalid_headings = '"' + '", "'.join(invalid_headings) + '"'
                raise csv.Error(
                    f"Unable to process CSV file: column headers must start with a letter and "
                    f"may only contain lowercase letters, numbers, and underscores. Invalid "
                    f"headers: {joined_invalid_headings}"
                )
            if not sample or not contents:
                raise csv.Error("no data found")

            column_types = cls.get_column_types(headers, contents)
            sample = list(
                zip(cls.make_unique_headers(headers), column_types, list(map(list, zip(*sample))))
            )
            return sample, None
        except (TabulatorException, csv.Error) as e:
            error_message = str(e)
            app.logger.error(error_message)
            return [], error_message

    @classmethod
    def check_and_clean_up_row(cls, extended_rows):
        """ Check if row length is same as header and standardize null values as empty strings """
        for row_number, headers, row in extended_rows:
            if len(row) != len(headers):
                raise csv.Error(
                    f'Unable to process CSV file: row {row_number} has a different number of data '
                    f'points ({len(row)}) than there are column headers ({len(headers)})'
                )
            cleaned_row = list(
                map(
                    lambda value: reduce(
                        lambda value, null_value: value.replace(null_value, ''),
                        CSV_NULL_VALUES,
                        value,
                    ),
                    row,
                )
            )
            yield (row_number, headers, cleaned_row)

    @classmethod
    def make_unique_headers(cls, headers):
        """ Headers with same name get a unique post fix number header_1, header_2, etc """
        if len(set(headers)) == len(headers):
            return headers
        post_fix_headers = []
        header_count = {i: headers.count(i) for i in headers}
        for i, name in enumerate(headers):
            post_fix_name = name
            if header_count[name] > 1:
                post_fix_name += '_' + str(headers[0:i].count(name) + 1)
            post_fix_headers.append(post_fix_name)
        return cls.make_unique_headers(post_fix_headers)

    @classmethod
    def get_column_types(cls, csv_headers, csv_contents):
        """Infers schema from CSV."""
        schema = Schema()
        schema.infer(
            [csv_headers] + csv_contents, confidence=1, headers=1,
        )
        fields = schema.descriptor['fields']
        column_types = cls.get_postgres_column_datatypes(fields, 'type')
        return column_types

    @classmethod
    def get_postgres_column_datatypes(cls, schema, key):
        """Convert schema to Postgres data types."""
        values = cls.extract_values(schema, key)
        for i, value in enumerate(values):
            if value == 'integer':
                values[i] = DataUploaderDataTypes.INTEGER.value
            elif value == 'boolean':
                values[i] = DataUploaderDataTypes.BOOLEAN.value
            elif value == 'date':
                values[i] = DataUploaderDataTypes.DATE.value
            elif value == 'datetime':
                values[i] = DataUploaderDataTypes.TIMESTAMP.value
            elif value == 'number':
                values[i] = DataUploaderDataTypes.NUMERIC.value
            else:
                values[i] = DataUploaderDataTypes.TEXT.value
        return values

    @classmethod
    def extract_values(cls, schema, key):
        values = []
        for row in schema:
            if key in row:
                values.append(row[key])
        return values
