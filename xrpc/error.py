# possible exceptions caused by the API calls
from datetime import datetime
from typing import Optional

from xrpc.util import time_now


class RPCError(Exception):
    def __str__(self):
        args = ', '.join(self.args)
        return f'{self.__class__.__name__}({args})'


class UndecidedError(RPCError):
    # We have no idea if the operation succeedeed or not
    # Operation state: UNDECIDED
    pass


class FailedError(RPCError):
    # Operation FAILED
    pass


class HorizonPassedError(UndecidedError):
    def __init__(self, when: datetime):
        self.when = when
        super().__init__(when)

    def __str__(self):
        now = time_now()

        total = self.when - now

        return f'{self.__class__.__name__}(Seconds={total.total_seconds():0.2f}s)'

    # the API server had already purged everything later than this specific TIME horizon.

    # Please note this does not provide any guarantees of the operation completion
    pass


class TimeoutError(UndecidedError):
    # if the configuration of the callable allows for terminating timeouts,
    # then this is the exception that is caused by them

    # Please note this does not provide any guarantees of the operation completion
    pass


class InternalError(FailedError):
    # a callable raised an exception
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


class InvalidFingerprintError(FailedError):
    # the call was made to a point that does not support given arguments
    def __init__(self, reason: Optional[str] = None):
        self.reason = reason
        super().__init__(reason)


class TerminationException(BaseException):
    def __init__(self, return_=None):
        self.return_ = return_
        super().__init__(self, return_)
