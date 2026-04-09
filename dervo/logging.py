import logging
import sys
import time
from typing import Tuple, Union

from dervo.misc import mkdir

log = logging.getLogger(__name__)

reasonable_formatters = {
    "extended": logging.Formatter(
        "%(asctime)s %(name)s %(funcName)s %(levelname)s: %(message)s",
        "%Y-%m-%d %H:%M:%S UTC",
    ),
    "short": logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S UTC"
    ),
    "shorter": logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S UTC"
    ),
    "shortest": logging.Formatter("%(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S UTC"),
}


def logging_init(
    stream_loglevel: int,
    formatter: Union[str, logging.Formatter] = "extended",
    stream_name: str = "stderr",
):
    """Create initial stream handler"""
    formatter = parse_formatter(formatter)
    if stream_name == "stderr":
        stream = sys.stderr
    elif stream_name == "stdout":
        stream = sys.stdout
    else:
        raise ValueError(f"Unknown {stream_name=}. Accepted values: stderr, stdout")
    # Get root logger (with NOTSET level)
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    logging.Formatter.converter = time.gmtime
    # Init stream handler
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)
    handler.setLevel(stream_loglevel)
    logger.addHandler(handler)
    return logger


def parse_loglevel(loglevel_arg) -> Tuple[int, str]:
    if isinstance(loglevel_arg, str):
        loglevel_int = logging._checkLevel(loglevel_arg)  # type: ignore[attr-defined]
        loglevel_str = loglevel_arg
    elif isinstance(loglevel_arg, int):
        loglevel_int = loglevel_arg
        loglevel_str = logging.getLevelName(loglevel_arg)
    else:
        raise RuntimeError(f"Can't parse {loglevel_arg=}")
    return loglevel_int, loglevel_str


def parse_formatter(formatter) -> logging.Formatter:
    if isinstance(formatter, str):
        return reasonable_formatters[formatter]
    if isinstance(formatter, list) and len(formatter) == 2:
        return logging.Formatter(*formatter)
    raise ValueError(
        f"formatter must be a preset name or [fmt, datefmt], got {formatter!r}"
    )


def add_filehandler(logfilepath, loglevel=logging.DEBUG, formatter="extended"):
    formatter = parse_formatter(formatter)
    out_filehandler = logging.FileHandler(str(logfilepath))
    out_filehandler.setFormatter(formatter)
    out_filehandler.setLevel(loglevel)
    logging.getLogger().addHandler(out_filehandler)
    return logfilepath


def clamp_package_loglevels(limit_packages: dict):
    """Set minimum log level per package group."""
    for level, packages in limit_packages.items():
        loglevel_int = parse_loglevel(level)[0]
        for pkg in packages:
            logging.getLogger(pkg).setLevel(loglevel_int)


def add_logging_filehandlers(workfolder, id_string, foldername, cfg_handlers):
    """Create logging file handlers per config."""
    assert isinstance(
        logging.getLogger().handlers[0], logging.StreamHandler
    ), "First handler should be StreamHandler"
    logfolder = mkdir(workfolder / foldername)
    handlers = {}
    for name, h in cfg_handlers.items():
        if h is None:
            continue
        loglevel_int, loglevel_str = parse_loglevel(h["loglevel"])
        logfilepath = logfolder / "{}.{}.log".format(id_string, h["suffix"])
        add_filehandler(
            logfilepath,
            loglevel_int,
            h["formatter"],
        )
        handlers[name] = {
            "loglevel_int": loglevel_int,
            "loglevel_str": loglevel_str,
            "logfilepath": logfilepath,
        }
    return handlers


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
                if record.levelno >= h.level:
                    h.handle(record)
