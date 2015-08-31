import time
import functools
import itertools as it

import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, ConnectionError
from circuit import CircuitOpenError
import logging

from .breaker import CircuitBreakerSet
from .exceptions import AllHostsUnreachableException, MaxRetriesReached


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Retriable(object):

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, **new_kwargs):
        kwargs = self.kwargs.copy()
        kwargs.update(new_kwargs)
        return self.func(*self.args, **kwargs)

    def __repr__(self):
        return '<Retriable (%s, %s, %s)>' % (self.func, self.args, self.kwargs)


class SmartClient(object):
    '''This client is used when dealing with existing client libraries that
    issue their own requests (whether HTTP or some other protocol). This client
    then provides

    To create an instance, you need to let it know about the types of errors
    that it can catch in order to retry. For HTTP, this is typically connection
    releated errors, however your library might wrap them.

    There are 2 ways to make a function retriable. The first is by creating a
    retriable object::

        # create the signature
        retriable = smart_client.Retriable(my_client.retrieve, id)

        # now execute it
        item = smart_client.retry(retriable, max_tries=5)

    The second is using retry() as as decorator:

        @smart_client.retry
        def get_object(id):
            return my_client.retrieve(id)

        @smart_client.retry(max_tries=5)
        def get_object(id):
            return my_client.retrieve(id)
    '''

    Retriable = Retriable

    def __init__(self, name, error_types=None, hosts_provider=None, hosts=None):
        self.name = name
        self.error_types = error_types
        self.breakers = CircuitBreakerSet(error_types or [])
        # TODO: Check that one and only one of these is specified.
        self._hosts_iter = it.cycle(hosts) if hosts else None
        self._hosts_provider = hosts_provider

    def get_hostname(self):
        if self._hosts_provider:
            return self._hosts_provider.get_hostname()
        else:
            return next(self._hosts_iter)

    def retry(self, retriable=None, max_tries=3, backoff=None, send_host=False):
        # Creating a decorator: @client.retry(max_tries=5)
        if retriable is None:
            return functools.partial(self.retry, max_tries=max_tries, backoff=backoff, send_host=send_host)

        # Being used as a decorator: @client.retry
        if not isinstance(retriable, Retriable):
            @functools.wraps(retriable)
            def wrapper(*args, **kwargs):
                _retriable = Retriable(retriable, *args, **kwargs)
                self.retry(_retriable, max_tries=max_tries, backoff=backoff, send_host=send_host)
            return wrapper

        # actually do the retries
        for i in xrange(1, max_tries + 1):
            host = self.get_hostname()
            try:
                logger.info('Attempting %s for host %s ...' % (retriable, host))
                with self.breakers.context(host):
                    if send_host:
                        return retriable(host=host)
                    else:
                        return retriable()
            except CircuitOpenError:
                if self.breakers.all_closed():
                    raise AllHostsUnreachable('All hosts unreachable for %s' % self.name)
                logger.warning('Silenced request failure for host %s due to CircuitOpenError.', host)
            except tuple(self.breakers.error_types) as e:
                logger.warning('Silenced %s for host %s.', e, host)

            if backoff and i < max_tries:  # Don't sleep after last attempt
                time.sleep(backoff() if callable(backoff) else backoff)

        raise MaxRetriesReached('Attempted %d times for %s' % (max_tries, self.name))


class SmartHTTPClient(SmartClient):
    '''A smart HTTP client that knows about the requests library's error types.
    You must provided a hosts_provider or a host list.
    '''
    HTTP_ERROR_TYPES = (ConnectTimeout, ReadTimeout, ConnectionError)

    def __init__(self, name, hosts_provider=None, hosts=None):
        super(SmartHTTPClient, self).__init__(name, self.HTTP_ERROR_TYPES, hosts_provider, hosts)
        # Keep a session per host
        self._sessions = {}

    def get(self, path, *args, **kwargs):
        req = self.Retriable(self._send, 'GET', path, *args, **kwargs)
        return self.retry(req, send_host=True)

    def _send(self, method, path, host=None, **kwargs):
        url = 'http://' + host + '/' + path.lstrip('/')
        session = self.get_session(host)
        prepared = session.prepare_request(requests.Request(method, url, **kwargs))
        return session.send(prepared)

    def get_session(self, hostname):
        session = self._sessions.get(hostname)
        if not session:
            session = self._sessions[hostname] = requests.Session()
        return session


