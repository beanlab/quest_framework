import signal

from .suspension import raise_suspension
from ..logs import quest_logger

_running_workflows = set()
_original_sig_handler = signal.getsignal(signal.SIGINT)


def _our_handler(sig, frame):
    quest_logger.debug(f'Caught KeyboardInterrupt: {sig}')
    for historian in _running_workflows:
        historian.signal_suspend()
    raise_suspension(True)


def track_workflow(historian):
    _running_workflows.add(historian)
    signal.signal(signal.SIGINT, _our_handler)


def untrack_workflow(historian):
    _running_workflows.remove(historian)
    if not _running_workflows:
        signal.signal(signal.SIGINT, _original_sig_handler)
