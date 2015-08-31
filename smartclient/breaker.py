import time
import logging
import circuit


class CircuitBreakerSet(circuit.CircuitBreakerSet):

    def __init__(self, errors, logger=None):
        logger = logger or logging.getLogger('circuit-breaker')
        super(CircuitBreakerSet, self).__init__(time.time, logger)
        self.handle_errors(errors)

    def test(self):
        '''Raises CircuitOpenError if any of them are open otherwise everying OK.'''
        return [circuit.test() for circuit in self.circuits.values()]

    def all_closed(self):
        try:
            self.test()
        except circuit.CircuitOpenError:
            return False
