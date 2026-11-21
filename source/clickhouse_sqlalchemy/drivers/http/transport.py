import json
import re

from datetime import datetime
from decimal import Decimal
from functools import partial

from ipaddress import IPv4Address, IPv6Address

import requests

from ...exceptions import DatabaseException
from .exceptions import HTTPException, StreamException
from .utils import parse_tsv


DEFAULT_DDL_TIMEOUT = None
STREAM_DEFAULT_FORMAT = 'JSONCompactEachRowWithNamesAndTypes'
DATE_NULL = '0000-00-00'
DATETIME_NULL = '0000-00-00 00:00:00'

EXTRACT_SUBTYPE_RE = re.compile(r'^[^\(]+\((.+)\)$')


def date_converter(x):
    if x != DATE_NULL:
        return datetime.strptime(x, '%Y-%m-%d').date()
    return None


def datetime_converter(x):
    if x == DATETIME_NULL:
        return None
    elif len(x) > 19:
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
    else:
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')


def nullable_converter(subtype_str, x):
    if x is None:
        return None

    converter = _get_type(subtype_str)
    return converter(x) if converter else x


def nothing_converter(x):
    return None


converters = {
    'Int8': int,
    'UInt8': int,
    'Int16': int,
    'UInt16': int,
    'Int32': int,
    'UInt32': int,
    'Int64': int,
    'UInt64': int,
    'Int128': int,
    'UInt128': int,
    'Int256': int,
    'UInt256': int,
    'Float32': float,
    'Float64': float,
    'Decimal': Decimal,
    'Date': date_converter,
    'DateTime': datetime_converter,
    'DateTime64': datetime_converter,
    'IPv4': IPv4Address,
    'IPv6': IPv6Address,
    'Nullable': nullable_converter,
    'Nothing': nothing_converter,
}


def _get_type(type_str):
    result = converters.get(type_str)
    if result is not None:
        return result
    # sometimes type_str is DateTime64(x)
    if type_str.startswith('DateTime64'):
        return converters['DateTime64']
    if type_str.startswith('Decimal'):
        return converters['Decimal']
    if type_str.startswith('Nullable('):
        subtype_str = EXTRACT_SUBTYPE_RE.match(type_str).group(1)
        return partial(converters['Nullable'], subtype_str)
    return None


class RequestsTransport(object):

    def __init__(
            self,
            db_url, db_name, username, password,
            timeout=None, ch_settings=None,
            **kwargs):

        self.db_url = db_url
        self.db_name = db_name
        self.auth = (username, password)
        self.timeout = float(timeout) if timeout is not None else None
        self.verify = kwargs.pop('verify', True)
        self.cert = kwargs.pop('cert', None)
        self.headers = {
            key[8:]: value
            for key, value in kwargs.items()
            if key.startswith('header__')
        }

        self.unicode_errors = kwargs.pop('unicode_errors', 'replace')

        ch_settings = dict(ch_settings or {})
        self.ch_settings = ch_settings

        self.ch_settings['default_format'] = 'TabSeparatedWithNamesAndTypes'

        ddl_timeout = kwargs.pop('ddl_timeout', DEFAULT_DDL_TIMEOUT)
        if ddl_timeout is not None:
            self.ch_settings['distributed_ddl_task_timeout'] = int(ddl_timeout)

        # By default, keep connection open between queries.
        http = kwargs.pop('http_session', requests.Session)
        self.http = http() if callable(http) else http

        super(RequestsTransport, self).__init__()

    def execute(self, query, params=None):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        r = self._send(query, params=params, stream=True)
        try:
            lines = r.iter_lines()
            try:
                names = parse_tsv(next(lines), self.unicode_errors)
                types = parse_tsv(next(lines), self.unicode_errors)
            except StopIteration:
                # Empty result; e.g. a DDL request.
                return

            convs = [_get_type(type_) for type_ in types]

            yield names
            yield types

            for line in lines:
                values = parse_tsv(line, self.unicode_errors)
                yield [
                    (conv(x) if conv else x)
                    for x, conv in zip(values, convs)
                ]
        finally:
            r.close()

    def execute_stream(self, query, params=None):
        """
        Strictly stream rows in JSONCompactEachRowWithNamesAndTypes format.

        Returns a `StreamedResult` yielding column names, column types and
        then data rows one by one. Unlike `execute`, a missing or malformed
        header, a row that is not a JSON array of the expected width, a
        non-data (e.g. error message) line or an interrupted connection
        raise `StreamException` carrying the number of rows parsed so far,
        instead of being silently treated as the end of the result.

        The HTTP response is released when the stream is exhausted, fails
        or is closed.
        """
        response = self._send(
            query, params=params, stream=True,
            default_format=STREAM_DEFAULT_FORMAT
        )
        return StreamedResult(response, self._iter_stream_rows(response))

    def _iter_stream_rows(self, response):
        rows_parsed = 0
        try:
            lines = response.iter_lines()

            try:
                names_line = next(lines)
            except StopIteration:
                # Empty result; e.g. a DDL request.
                return

            names = self._parse_stream_header(names_line)

            try:
                types_line = next(lines)
            except StopIteration:
                raise StreamException(
                    'Missing column types header in streamed response',
                    rows_parsed
                )
            types = self._parse_stream_header(types_line)

            if len(names) != len(types):
                raise StreamException(
                    'Column names/types header mismatch: '
                    '{} names vs {} types'.format(len(names), len(types)),
                    rows_parsed
                )

            convs = [_get_type(type_) for type_ in types]
            width = len(names)

            yield names
            yield types

            for line in lines:
                if not line:
                    # Tolerate blank lines, e.g. a trailing newline.
                    continue

                values = self._parse_stream_row(line, rows_parsed)
                if len(values) != width:
                    raise StreamException(
                        'Expected {} columns in streamed row, '
                        'got {}'.format(width, len(values)),
                        rows_parsed
                    )

                try:
                    row = [
                        (conv(value) if conv is not None else value)
                        for value, conv in zip(values, convs)
                    ]
                except Exception as exc:
                    raise StreamException(
                        'Cannot convert streamed row: {}'.format(exc),
                        rows_parsed
                    )

                rows_parsed += 1
                yield row
        except StreamException:
            raise
        except Exception as exc:
            # E.g. the connection was interrupted mid-stream
            # (requests.exceptions.ChunkedEncodingError and friends).
            raise StreamException(
                'Streamed response interrupted: {}'.format(exc),
                rows_parsed
            ) from exc
        finally:
            response.close()

    def _parse_stream_header(self, line):
        values = self._parse_stream_json(line, 'header')
        if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values):
            raise StreamException(
                'Invalid header in streamed response: '
                'expected a JSON array of strings'
            )
        return values

    def _parse_stream_row(self, line, rows_parsed):
        values = self._parse_stream_json(line, 'row', rows_parsed)
        if not isinstance(values, list):
            raise StreamException(
                'Invalid streamed row: expected a JSON array',
                rows_parsed
            )
        return values

    def _parse_stream_json(self, line, what, rows_parsed=0):
        try:
            text = line.decode('utf-8', errors=self.unicode_errors)
            # parse_float=Decimal keeps Decimal values exact; Python ints
            # are arbitrary precision, so big integers survive as well.
            return json.loads(text, parse_float=Decimal)
        except ValueError as exc:
            raise StreamException(
                'Invalid JSON in streamed {}: {}'.format(what, exc),
                rows_parsed
            )

    def raw(self, query, params=None, stream=False):
        """
        Performs raw query to database. Returns its output
        :param query: Query to execute
        :param params: Additional params should be passed during query.
        :param stream: If flag is true, Http response from ClickHouse will be
            streamed.
        :return: Query execution result
        """
        return self._send(query, params=params, stream=stream).text

    def _send(self, data, params=None, stream=False, default_format=None):
        data = data.encode('utf-8')
        params = params or {}
        params['database'] = self.db_name
        params.update(self.ch_settings)
        if default_format is not None:
            params['default_format'] = default_format

        # TODO: retries, prepared requests
        r = self.http.post(
            self.db_url, auth=self.auth, params=params, data=data,
            stream=stream, timeout=self.timeout, headers=self.headers,
            verify=self.verify, cert=self.cert
        )
        if r.status_code != 200:
            orig = HTTPException(r.text)
            orig.code = r.status_code
            raise DatabaseException(orig)
        return r


class StreamedResult(object):
    """
    Iterator over a strictly streamed HTTP response: column names, column
    types, then data rows.

    `close()` releases the underlying HTTP response. The response is also
    released automatically once the stream is exhausted or fails, so
    `close()` only needs to be called when abandoning a partially consumed
    stream.
    """

    def __init__(self, response, rows):
        self.response = response
        self._rows = rows

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._rows)

    def close(self):
        self._rows.close()
        self.response.close()
