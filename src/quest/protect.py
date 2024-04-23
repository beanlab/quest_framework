import signal
from threading import get_ident
from platform import platform
import logging

class Protection():
    def __init__(self):
        self._implements_signals = True
        if "Windows" in platform():
            self._implements_signals = False

        self._pertinent_signals = [signal.SIGABRT, signal.SIGINT, signal.SIGTERM]
        self._old_signal_mask = None

    def __enter__(self):
        if self._implements_signals:
            self._old_signal_mask = signal.pthread_sigmask(signal.SIG_BLOCK, self._pertinent_signals)
            logging.debug(f"Protecting from {self._pertinent_signals}")
        return self

    def __exit__(self, type, value, traceback):
        if self._implements_signals:
            signal.pthread_sigmask(signal.SIG_SETMASK, self._old_signal_mask)
            logging.debug(f"Unprotected from {self._pertinent_signals}")