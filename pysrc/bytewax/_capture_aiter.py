from collections import abc


class CaptureAiter(abc.AsyncIterator, abc.Awaitable, abc.Generator):
    def __anext__(self):
        return self

    def __await__(self):
        return self

    def send(self, value):
        if value is not None:
            raise StopIteration(value)
        return None

    def throw(self, typ, value=None, traceback=None):
        if value is None:
            if traceback is None:
                raise typ
            value = typ()
        if traceback is not None:
            value = value.with_traceback(traceback)
        raise value
