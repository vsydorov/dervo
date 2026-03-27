import os.path
from os import PathLike
from pathlib import Path

StrPath = str | PathLike[str]


def mkdir(directory: StrPath) -> Path:
    """
    Python 3.5 pathlib shortcut to mkdir -p
    Fails if parent is created by other process in the middle of the call
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def abspath(path: StrPath):
    return Path(os.path.abspath(path))


def normpath(path: StrPath):
    return Path(os.path.normpath(path))
