"""
Tests for the strict streaming mode of the HTTP driver
(`stream='json'`, JSONCompactEachRowWithNamesAndTypes).

These tests are self-contained: no ClickHouse server is required.
"""
from decimal import Decimal
from unittest import TestCase

import requests
import responses
from sqlalchemy import create_engine, text

from clickhouse_sqlalchemy.drivers.http import connector
from clickhouse_sqlalchemy.drivers.http.exceptions import StreamException
from clickhouse_sqlalchemy.drivers.http.transport import (
    RequestsTransport, STREAM_DEFAULT_FORMAT
)


class FakeResponse(object):
    """Minimal stand-in for requests.Response (stream=True)."""

    def __init__(self, lines=(), error=None):
        self.lines = list(lines)
        self.error = error
        self.closed = False

    def iter_lines(self):
        for line in self.lines:
            yield line
        if self.error is not None:
            raise self.error

    def close(self):
        self.closed = True


def make_transport(response):
    """
    Real RequestsTransport with `_send` stubbed to return `response`.
    Records `_send` calls in `transport.send_calls`.
    """
    transport = RequestsTransport(
        'http://localhost:8123', 'default', 'user', 'password'
    )
    calls = []

    def fake_send(query, params=None, stream=False, default_format=None):
        calls.append({
            'query': query,
            'params': params,
            'stream': stream,
            'default_format': default_format,
        })
        return response

    transport._send = fake_send
    transport.send_calls = calls
    return transport


def make_connection(response, **kwargs):
    conn = connector.Connection(
        'http://localhost:8123', 'default', 'user', 'password', **kwargs
    )
    conn.transport._send = lambda *args, **kw: response
    return conn


HEADER = [b'["a", "b", "c", "d"]',
          b'["UInt64", "Decimal(8, 2)", "String", "Nullable(Int64)"]']


class ExecuteStreamTestCase(TestCase):
    def test_requests_json_format(self):
        transport = make_transport(FakeResponse())
        list(transport.execute_stream('SELECT 1'))

        call, = transport.send_calls
        self.assertTrue(call['stream'])
        self.assertEqual(call['default_format'], STREAM_DEFAULT_FORMAT)

    def test_parses_names_types_and_rows(self):
        response = FakeResponse(HEADER + [
            b'[18446744073709551615, 1.10, "hello", null]',
            b'["42", "2.5", "world", "123"]',
        ])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')

        self.assertEqual(next(result), ['a', 'b', 'c', 'd'])
        self.assertEqual(
            next(result),
            ['UInt64', 'Decimal(8, 2)', 'String', 'Nullable(Int64)']
        )
        # Big integers, Decimals, strings and NULLs are preserved,
        # quoted or not.
        self.assertEqual(
            next(result),
            [18446744073709551615, Decimal('1.10'), 'hello', None]
        )
        self.assertEqual(next(result), [42, Decimal('2.5'), 'world', 123])
        self.assertRaises(StopIteration, next, result)

    def test_float_stays_float(self):
        response = FakeResponse([
            b'["a"]', b'["Float64"]', b'[1.5]', b'["2.5"]'
        ])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)

        row = next(result)
        self.assertEqual(row, [1.5])
        self.assertIsInstance(row[0], float)
        row = next(result)
        self.assertEqual(row, [2.5])
        self.assertIsInstance(row[0], float)

    def test_empty_result_is_not_an_error(self):
        # E.g. a DDL request.
        response = FakeResponse()
        transport = make_transport(response)

        self.assertEqual(list(transport.execute_stream('DDL')), [])
        self.assertTrue(response.closed)

    def test_short_row_raises(self):
        response = FakeResponse(HEADER + [
            b'[1, 1.1, "x", null]',
            b'[2, 2.2, "y"]',
        ])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)
        self.assertEqual(next(result), [1, Decimal('1.1'), 'x', None])

        with self.assertRaises(StreamException) as ctx:
            next(result)
        self.assertEqual(ctx.exception.rows_parsed, 1)
        self.assertIn('rows parsed: 1', str(ctx.exception))
        self.assertTrue(response.closed)

    def test_row_with_extra_columns_raises(self):
        response = FakeResponse(HEADER + [b'[1, 1.1, "x", null, 5]'])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)

        with self.assertRaises(StreamException) as ctx:
            next(result)
        self.assertEqual(ctx.exception.rows_parsed, 0)

    def test_error_line_raises(self):
        # ClickHouse reports query failures that happen after the first
        # data block by appending the exception text to the response.
        response = FakeResponse([
            b'["a"]',
            b'["UInt64"]',
            b'[1]',
            b'[2]',
            b"Code: 81. DB::Exception: Database foo doesn't exist.",
        ])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)
        self.assertEqual(next(result), [1])
        self.assertEqual(next(result), [2])

        with self.assertRaises(StreamException) as ctx:
            next(result)
        self.assertEqual(ctx.exception.rows_parsed, 2)
        self.assertTrue(response.closed)

    def test_missing_names_header_raises(self):
        response = FakeResponse([b"Code: 81. DB::Exception: nope."])
        transport = make_transport(response)

        with self.assertRaises(StreamException) as ctx:
            next(transport.execute_stream('SELECT'), None)
        self.assertEqual(ctx.exception.rows_parsed, 0)
        self.assertTrue(response.closed)

    def test_missing_types_header_raises(self):
        response = FakeResponse([b'["a"]'])
        transport = make_transport(response)

        with self.assertRaises(StreamException) as ctx:
            next(transport.execute_stream('SELECT'), None)
        self.assertEqual(ctx.exception.rows_parsed, 0)
        self.assertTrue(response.closed)

    def test_non_string_header_raises(self):
        response = FakeResponse([b'[1, 2]', b'["UInt8", "UInt8"]'])
        transport = make_transport(response)

        with self.assertRaises(StreamException):
            next(transport.execute_stream('SELECT'), None)

    def test_header_length_mismatch_raises(self):
        response = FakeResponse([b'["a", "b"]', b'["UInt8"]'])
        transport = make_transport(response)

        with self.assertRaises(StreamException) as ctx:
            next(transport.execute_stream('SELECT'), None)
        self.assertEqual(ctx.exception.rows_parsed, 0)

    def test_non_array_row_raises(self):
        response = FakeResponse([b'["a"]', b'["UInt8"]', b'{"a": 1}'])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)

        with self.assertRaises(StreamException) as ctx:
            next(result)
        self.assertEqual(ctx.exception.rows_parsed, 0)

    def test_connection_interrupted_raises(self):
        error = requests.exceptions.ChunkedEncodingError(
            'Response ended prematurely'
        )
        response = FakeResponse(
            [b'["a"]', b'["UInt64"]', b'[1]', b'[2]'], error=error
        )
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)
        next(result)
        self.assertEqual(next(result), [1])
        self.assertEqual(next(result), [2])

        with self.assertRaises(StreamException) as ctx:
            next(result)
        # Not silently treated as the end of the result.
        self.assertEqual(ctx.exception.rows_parsed, 2)
        self.assertIs(ctx.exception.__cause__, error)
        self.assertTrue(response.closed)

    def test_close_releases_response(self):
        response = FakeResponse(HEADER + [b'[1, 1.1, "x", null]'])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        next(result)

        result.close()
        self.assertTrue(response.closed)
        self.assertRaises(StopIteration, next, result)

    def test_exhaustion_releases_response(self):
        response = FakeResponse(HEADER + [b'[1, 1.1, "x", null]'])
        transport = make_transport(response)
        result = transport.execute_stream('SELECT')
        list(result)
        self.assertTrue(response.closed)


class StreamJsonCursorTestCase(TestCase):
    def test_stream_option_parsing(self):
        conn = make_connection(FakeResponse(), stream='json')
        self.assertTrue(conn._stream_json)
        self.assertFalse(conn._prefetch)

        conn = make_connection(FakeResponse(), stream='JSON')
        self.assertTrue(conn._stream_json)

        conn = make_connection(FakeResponse(), stream='1')
        self.assertFalse(conn._stream_json)
        self.assertFalse(conn._prefetch)

        conn = make_connection(FakeResponse())
        self.assertFalse(conn._stream_json)
        self.assertTrue(conn._prefetch)

    def test_fetchmany_consumes_on_demand(self):
        response = FakeResponse([
            b'["a"]', b'["UInt64"]',
            b'[1]', b'[2]', b'[3]', b'[4]', b'[5]',
        ])
        conn = make_connection(response, stream='json')
        cursor = conn.cursor()
        cursor.execute('SELECT')

        self.assertEqual(cursor.description, [
            ('a', 'UInt64', None, None, None, None, True)
        ])
        self.assertEqual(cursor.fetchmany(2), [[1], [2]])
        self.assertEqual(cursor.fetchone(), [3])
        self.assertEqual(cursor.fetchall(), [[4], [5]])
        self.assertIsNone(cursor.fetchone())
        self.assertTrue(response.closed)

    def test_cursor_close_releases_response(self):
        response = FakeResponse([b'["a"]', b'["UInt64"]', b'[1]', b'[2]'])
        conn = make_connection(response, stream='json')
        cursor = conn.cursor()
        cursor.execute('SELECT')
        self.assertEqual(cursor.fetchone(), [1])

        cursor.close()
        self.assertTrue(response.closed)
        self.assertRaises(RuntimeError, cursor.fetchone)

    def test_reexecute_releases_previous_response(self):
        first = FakeResponse([b'["a"]', b'["UInt64"]', b'[1]'])
        second = FakeResponse([b'["b"]', b'["UInt64"]', b'[2]'])
        responses = iter([first, second])

        conn = make_connection(None, stream='json')
        conn.transport._send = lambda *args, **kw: next(responses)

        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.execute('SELECT 2')

        self.assertTrue(first.closed)
        self.assertFalse(second.closed)
        self.assertEqual(cursor.fetchall(), [[2]])

    def test_error_mid_fetch_raises_and_releases(self):
        response = FakeResponse([
            b'["a"]', b'["UInt64"]', b'[1]', b'[2]', b'Code: 81. boom',
        ])
        conn = make_connection(response, stream='json')
        cursor = conn.cursor()
        cursor.execute('SELECT')

        with self.assertRaises(StreamException) as ctx:
            cursor.fetchall()
        self.assertEqual(ctx.exception.rows_parsed, 2)
        self.assertTrue(response.closed)

    def test_empty_result(self):
        response = FakeResponse()
        conn = make_connection(response, stream='json')
        cursor = conn.cursor()
        cursor.execute('DDL')

        self.assertEqual(cursor.fetchall(), [])
        self.assertTrue(response.closed)

    def test_tsv_streaming_still_works(self):
        response = FakeResponse([
            b'a', b'UInt64', b'1', b'2',
        ])
        conn = make_connection(response, stream='1')
        cursor = conn.cursor()
        cursor.execute('SELECT')

        self.assertEqual(cursor.fetchall(), [[1], [2]])
        self.assertTrue(response.closed)

    def test_prefetch_by_default(self):
        response = FakeResponse([
            b'a', b'UInt64', b'1', b'2',
        ])
        conn = make_connection(response)
        cursor = conn.cursor()
        cursor.execute('SELECT')

        self.assertEqual(cursor.fetchall(), [[1], [2]])
        self.assertTrue(response.closed)


class StreamJsonEngineTestCase(TestCase):
    url = 'http://localhost:8123/'

    def add_init_responses(self, fmt):
        if fmt == 'json':
            version = '["version()"]\n["String"]\n["24.1.1.1"]\n'
            database = '["currentDatabase()"]\n["String"]\n["default"]\n'
        else:
            version = 'version()\nString\n24.1.1.1\n'
            database = 'currentDatabase()\nString\ndefault\n'
        responses.add(responses.POST, self.url, body=version)
        responses.add(responses.POST, self.url, body=database)

    @responses.activate
    def test_end_to_end(self):
        self.add_init_responses('json')
        responses.add(
            responses.POST, self.url,
            body='["x"]\n["UInt64"]\n[1]\n[2]\n'
        )

        engine = create_engine(
            'clickhouse+http://localhost:8123/default?stream=json'
        )
        with engine.connect() as conn:
            rv = conn.execute(text('SELECT x')).fetchall()
        self.assertEqual(rv, [(1,), (2,)])

        for call in responses.calls:
            self.assertIn(
                'default_format=' + STREAM_DEFAULT_FORMAT,
                call.request.url
            )

    @responses.activate
    def test_mid_stream_error_propagates(self):
        self.add_init_responses('json')
        responses.add(
            responses.POST, self.url,
            body='["x"]\n["UInt64"]\n[1]\nCode: 81. DB::Exception: boom\n'
        )

        engine = create_engine(
            'clickhouse+http://localhost:8123/default?stream=json'
        )
        with engine.connect() as conn:
            with self.assertRaises(StreamException) as ctx:
                conn.execute(text('SELECT x')).fetchall()
        self.assertEqual(ctx.exception.rows_parsed, 1)

    @responses.activate
    def test_default_tsv_unchanged(self):
        self.add_init_responses('tsv')
        responses.add(
            responses.POST, self.url,
            body='x\nUInt64\n1\n2\n'
        )

        engine = create_engine('clickhouse+http://localhost:8123/default')
        with engine.connect() as conn:
            rv = conn.execute(text('SELECT x')).fetchall()
        self.assertEqual(rv, [(1,), (2,)])

        for call in responses.calls:
            self.assertIn(
                'default_format=TabSeparatedWithNamesAndTypes',
                call.request.url
            )
