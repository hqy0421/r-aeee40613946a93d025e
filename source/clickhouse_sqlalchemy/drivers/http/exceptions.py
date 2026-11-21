
class HTTPException(Exception):
    code = None


class StreamException(Exception):
    """
    A strictly streamed response is malformed or was interrupted.

    Raised instead of silently treating the data received so far as the
    complete result. `rows_parsed` is the number of data rows that were
    successfully parsed before the failure.
    """

    def __init__(self, message, rows_parsed=0):
        self.rows_parsed = rows_parsed
        super(StreamException, self).__init__(
            '{} (rows parsed: {})'.format(message, rows_parsed)
        )
