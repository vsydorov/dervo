import logging
from typing import Union

log = logging.getLogger(__name__)

reasonable_formatters = {
    "extended": logging.Formatter(
        "%(asctime)s %(name)s %(funcName)s %(levelname)s: %(message)s",
        "%Y-%m-%d %H:%M:%S",
    ),
    "short": logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    ),
    "shorter": logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    ),
    "shortest": logging.Formatter("%(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S"),
}


def reasonable_logging_setup(stream_loglevel: int, formatter="extended"):
    """Create STDOUT stream handler, curtail spam"""
    if isinstance(formatter, str):
        formatter = reasonable_formatters[formatter]
    # Get root logger (with NOTSET level)
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    # Stream handler takes 'loglevel'
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(stream_loglevel)
    logger.addHandler(handler)
    # Prevent some spammy packages from exceeding INFO verbosity
    spammy_packages = [
        "PIL",
        "git",
        "tensorflow",
        "matplotlib",
        "selenium",
        "urllib3",
        "fiona",
        "rasterio",
    ]
    for packagename in spammy_packages:
        logging.getLogger(packagename).setLevel(max(logging.INFO, stream_loglevel))
    return logger


def docopt_loglevel(loglevel: Union[str, int]) -> int:
    """Tries to get int value softly.
    For parsing docopt argument
    """
    try:
        loglevel_int = int(loglevel)
    except ValueError:
        assert isinstance(loglevel, str)
        loglevel_int = loglevel_str_to_int(loglevel)
    return loglevel_int


def loglevel_str_to_int(loglevel: str) -> int:
    assert isinstance(loglevel, str)
    return logging._checkLevel(loglevel)  # type: ignore


def loglevel_int_to_str(loglevel: int) -> str:
    assert isinstance(loglevel, int)
    return logging.getLevelName(loglevel)


def add_filehandler(logfilename, level=logging.DEBUG, formatter="extended"):
    if isinstance(formatter, str):
        formatter = reasonable_formatters[formatter]
    out_filehandler = logging.FileHandler(str(logfilename))
    out_filehandler.setFormatter(formatter)
    out_filehandler.setLevel(level)
    logging.getLogger().addHandler(out_filehandler)
    return logfilename


class CaptureLogRecordsHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        self.captured_records = []

    def emit(self, record):
        self.captured_records.append(record)

    def close(self):
        logging.Handler.close(self)


class LogCaptorToRecords(object):
    """Capture log records while optionally pausing handlers.

    pause='none' — capture alongside all active handlers; handle_captured() is a no-op
    pause='all'  — pause all handlers; handle_captured() replays to all current handlers
    pause='file' — pause only FileHandlers, keep stream (stdout) active;
                   handle_captured() replays only to current FileHandlers
    """

    def __init__(self, pause="none"):
        if pause not in ("none", "all", "file"):
            raise ValueError(f"pause must be 'none', 'all', or 'file', got {pause!r}")
        self.pause = pause
        self._logger = logging.getLogger()
        self._captor_handler = CaptureLogRecordsHandler()
        self._paused_handlers = []
        self.captured = []

    def __enter__(self):
        if self.pause == "all":
            self._paused_handlers = self._logger.handlers.copy()
        elif self.pause == "file":
            self._paused_handlers = [
                h for h in self._logger.handlers if isinstance(h, logging.FileHandler)
            ]
        for h in self._paused_handlers:
            self._logger.removeHandler(h)
        self._logger.addHandler(self._captor_handler)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._logger.removeHandler(self._captor_handler)
        for h in self._paused_handlers:
            if h not in self._logger.handlers:
                self._logger.addHandler(h)
        self.captured = self._captor_handler.captured_records[:]
        if exc_type is not None:
            log.error(
                "<<(CAPTURED BEGIN)>> Capturer encountered an "
                "exception and released captured records"
            )
            self.handle_captured()
            log.error("<<(CAPTURED END)>> End of captured records")

    def handle_captured(self):
        if self.pause == "none":
            return  # All handlers were active during capture, no replay needed
        elif self.pause == "all":
            targets = self._logger.handlers.copy()
        else:  # 'file'
            targets = [
                h for h in self._logger.handlers if isinstance(h, logging.FileHandler)
            ]
        for record in self.captured:
            for h in targets:
                h.handle(record)
