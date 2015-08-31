import abc


class BackendBase(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def supports_watching(self):
        '''Return True if this backend supports watching for key changes (and
        therfore should implement watch_node() and watch_children().
        '''
        pass

    @abc.abstractproperty
    def client(self):
        '''Return the raw underlying client. If the client requires "starting",
        this should handle that.
        '''

    @abc.abstractmethod
    def get_node(self, name):
        pass

    @abc.abstractmethod
    def get_children(self, name):
        pass


class WatchableBase(BackendBase):

    supports_watching = True

    @abc.abstractmethod
    def watch_node(self, name, callback):
        '''Watches for changes in the node's value. Returns an instance of
        WatcherBase() which has a single method, stop().
        '''
        pass

    @abc.abstractmethod
    def watch_children(self, name, callback):
        '''Watches for changes in the child list of a given node. Returns an
        instance of WatcherBase() which has a single method, stop().
        '''
        pass

    @abc.abstractmethod
    def watch_all(self, name, callback):
        '''Watches for changes in the child list as well as watches each child
        node's values. Returns an instance of WatcherBaes() which has a single
        method, stop().

        # TODO: need to conform the data passed into callbacks from watchers.
        '''
        pass


class NonWatchableBase(BackendBase):

    supports_watching = False

    @abc.abstractproperty
    def update_threshold(self):
        raise NotImplementedError('Please define an update threshold for this backend.')


class WatcherBase(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def stop(self):
        '''Stops the watcher.'''
        pass
