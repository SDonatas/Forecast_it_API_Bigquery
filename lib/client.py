import logging
from functools import partialmethod
from json import JSONDecodeError
import json
from urllib.parse import urljoin
import requests

#Logger
logger = logging.getLogger(__name__)


class ForecastApiError(Exception):
    def __init__(self, message, status):
        super().__init__(message)

        self.message = message
        self.status = status

    def __str__(self):
        return 'HTTP %s: %s' % (self.status, self.message)


class InvalidAuthTokenError(ForecastApiError):
    pass


class NotFoundError(ForecastApiError):
    pass


class PayloadError(ForecastApiError):
    pass


class ServerError(ForecastApiError):
    pass


class ForecastApiClient:
    def __init__(self, url, token):
        self.session = requests.Session()
        self.url = url
        self._token = None
        self.token = token

        self.session.hooks['response'].append(self.response_hook)

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, token):
        self._token = token
        self.session.headers['X-FORECAST-API-KEY'] = token

    def response_hook(self, response, *args, **kwargs):
        status = response.status_code

        if status < 400:
            return

        try:
            message = response.json()['message']
        except (UnicodeDecodeError, JSONDecodeError):
            logger.debug(
            'Failed to json-decode response: %s\n%r',
            response.url, response.content,
            )
            raise ServerError('Invalid JSON response', status)
        except KeyError:
            logger.debug(
                'Missing error message: %s\n%r',
                response.url, response.content,
            )
            raise ServerError('Unknown error', status)

        if status < 500:
            raise {
                400: PayloadError,
                401: InvalidAuthTokenError,
                404: NotFoundError,
            }.get(
                response.status_code,
                ForecastApiError,
            )(message, status)

        raise ServerError(message, status)

    def prepare_request(self, method, path, **kwargs):
        request = requests.Request(method, urljoin(self.url, path), **kwargs)
        return self.session.prepare_request(request)

    def request(self, method, path, **kwargs):
        response = self.session.request(method, urljoin(self.url, path), **kwargs)

        try:
            return json.loads(response.text)
        except Exception as e:
            logger("Error parsing json")

    get = partialmethod(request, 'get', allow_redirects=True)
    options = partialmethod(request, 'options', allow_redirects=True)
    head = partialmethod(request, 'head', allow_redirects=False)
    post = partialmethod(request, 'post')
    put = partialmethod(request, 'put')
    patch = partialmethod(request, 'patch')
    delete = partialmethod(request, 'delete')
