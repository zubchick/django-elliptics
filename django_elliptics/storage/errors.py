# coding: utf-8


class BaseError(Exception):
    """Generic error for EllipticsStorage backend."""


class ModeError (BaseError):
    """File operation incompatible with file access mode."""


class HTTPError(BaseError):
    """Elliptics request failed."""


class SaveError(HTTPError):
    """Failed to store file to the backend."""

    def __str__(self):
        response = self.args[0]
        return 'got status code %s while sending to %s' % (
            response.status_code, response.url)


class ReadError(HTTPError):
    """Failed to read from the backend."""

    def __str__(self):
        response = self.args[0]
        return 'got status code %s while reading %s' % (
            response.status_code, response.url)


class TimeoutError(ReadError, SaveError):
    """Timeout error."""

    # ReadError and SaveError override __str__, because they get object with a
    # response to the input. In TimeoutError is impossible to pass the object
    # with a response, therefore overriding is introduced again.
    def __str__(self):
        return super(HTTPError, self).__str__()
