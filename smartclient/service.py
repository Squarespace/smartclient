import abc
import time
import random
import itertools as it
from .exceptions import NoHostsAvailableException


class BaseServiceProvider(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_host(self):
        '''Returns a dict representing a host's config.'''
        pass

    @abc.abstractmethod
    def get_hostname(self):
        '''Returns a valid hostname.'''
        pass


def default_hostname_adapter(config):
    port = config.get('sslPort', 0)
    if port == 0:
        port = config['port']
    return '{scheme}://{address}:{port}'.format(
        scheme='https' if config.get('sslPort', 0) != 0 else 'http',
        address=config['address'],
        port=port)


class BackendServiceProvider(BaseServiceProvider):
    '''One instance per "service".'''

    def __init__(self, name, backend, node=None, hostname_adapter=default_hostname_adapter):
        self.name = name
        self.backend = backend
        self._watcher = None
        self._node = node or name

        self._loaded = False
        self._hosts = []
        self._hosts_iter = UpdateableIterator(self._hosts, NoHostsAvailableException)

        self._last_updated = None
        self._hostname_adapter = hostname_adapter

    def get_hostname(self):
        return self._hostname_adapter(self.get_host())

    def get_host(self):
        if not self.backend.supports_watching:
            if time.time() - self._last_udpated > self.backend.update_threshold:
                self._reset_hosts(self._get_hosts())
        return next(self._hosts_iterator)

    @property
    def _hosts_iterator(self):
        '''Returns an infinite iterable that iterates over available hosts. The
        hosts lists gets updated on the fly.
        '''
        # initial load
        if not self._loaded:
            self._load()

        # Setup watching for backends that support it the first time this
        # iterator is accessed.
        if self.backend.supports_watching and self._watcher is None:
            self._watch()

        return self._hosts_iter

    def _load(self):
        self._reset_hosts(self._get_hosts())
        self._loaded = True

    def _reset_hosts(self, hosts):
        self._hosts = hosts
        # We randomize the hosts here in case our backend doesn't support
        # watching. Otherwise, everytime we update our hosts iterator, they
        # could be returned in the same order so we'd more heavily use the
        # beginning of the list. The other option of comparing new hosts and
        # old hosts and not actually updating the iterator won't work unless we
        # also compare the host configs as well...
        random.shuffle(self._hosts)
        self._hosts_iter.modify(self._hosts)
        self._last_updated = time.time()

    def _get_hosts(self):
        hosts = []
        for child in self.backend.get_children(self._node):
            hosts.append(self.backend.get_node(self._node + '/' + child))
        return hosts

    def _watch(self):
        self._watcher = self.backend.watch_all(self._node, self._on_children_change)

    def _on_children_change(self, children):
        self._reset_hosts(children.values())

    def _unwatch(self):
        # TODO: resuse the watcher?
        self._watcher.stop()
        self._watcher = None


class UpdateableIterator(object):

    def __init__(self, items, stop_exc=StopIteration):
        self.items = items
        self._iter = it.cycle(items)
        self._stop_exc = stop_exc

    def __iter__(self):
        return self

    def modify(self, items):
        self.items = items
        self._iter = it.cycle(items)

    def next(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise self._stop_exc()
