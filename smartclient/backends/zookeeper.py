import json
from kazoo.client import KazooClient
from .base import WatchableBase, WatcherBase


class ZooKeeper(WatchableBase):
    '''A zookeeper client wrapper. Improves usability especially for watcher callbacks.'''

    def __init__(self, *hosts):
        self._client = KazooClient(','.join(hosts), read_only=True)
        self._started = False

    @property
    def client(self):
        if not self._started:
            self._start()
        return self._client

    def _start(self):
        self._client.start()
        self._started = True

    def get_node(self, name):
        data, stat = self.client.get(name)
        return json.loads(data) if data != 'false' else {}

    def get_children(self, name):
        return self.client.get_children(name)

    def watch_node(self, name, callback):
        return KazooWatcher(self.client.DataWatch(name, func=callback))

    def watch_children(self, name, callback):
        return KazooWatcher(self.client.ChildrenWatch(name, func=callback))

    def watch_all(self, name, callback):
        '''callback should accept a single param, children, whcih will be a
        list of each child nodes data.
        '''
        def on_children_change(children):
            nodes = {child: self.get_node(node_join(name, child)) for child in children}
            callback(nodes)

        # This is kind of dumb for now, but basically we want to conform the
        # callback signature to a single list of children.
        def on_node_change(data, stat):
            # TODO: perhaps some sort of cache here? Otherwise we call
            # get_node() a lot...
            # We don't care about the individual change, just get all configs
            # as they are presently.
            nodes = {child: self.get_node(node_join(name, child)) for child in self.get_children(name)}
            callback(nodes)

        # Setup child watcher with our own callback which gets updated node
        # values.
        watches = [self.watch_children(name, on_children_change)]

        # TODO: This might be problematic, because it's going to make new node
        # watchers every time the list gets refreshed. Maybe need to keep
        # state so we don't create a ton of objects?
        for child in self.get_children(name):
            watches.append(self.watch_node(node_join(name, child), on_node_change))

        return KazooWatcher(*watches)


class KazooWatcher(WatcherBase):

    def __init__(self, *watches):

        self._watches = watches

    def stop(self):
        for watch in self._watches:
            self._watch._stopped = True


def node_join(*names):
    return '/'.join(name.rstrip('/') for name in names)
