class ClientException(Exception):
    pass


class NoHostsAvailableException(ClientException):
    pass


class AllHostsUnreachableException(ClientException):
    pass


class MaxRetriesReached(ClientException):
    pass
