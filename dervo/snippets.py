"""
Organization tools that do not concert experiments
"""
import subprocess
import logging
import platform
import math
import yaml
import numpy as np
import collections

from pathlib import Path
from typing import (  # NOQA
        Union, Any, NamedTuple, List, Tuple, Callable, TypeVar, Iterator,
        Iterable, Sequence)

from vst import small


log = logging.getLogger(__name__)


def loglevel_str_to_int(loglevel: str) -> int:
    assert isinstance(loglevel, str)
    return logging._checkLevel(loglevel)  # type: ignore


def loglevel_int_to_str(loglevel: int) -> str:
    assert isinstance(loglevel, int)
    return logging.getLevelName(loglevel)


def docopt_loglevel(loglevel) -> int:
    """Tries to get int value softly.
    For parsing docopt argument
    """
    try:
        loglevel_int = int(loglevel)
    except ValueError:
        loglevel_int = loglevel_str_to_int(loglevel)
    return loglevel_int


def find_exp_path(path_: str) -> Path:
    path = Path(path_)
    assert path.exists(), f'Path must exists: {path}'
    if path.is_file():
        log.warning('File instead of dir was provided, using its parent instead')
        path = path.parent
    return path


def platform_info():
    platform_string = f'Node: {platform.node()}'
    oar_jid = subprocess.run('echo $OAR_JOB_ID', shell=True,
            stdout=subprocess.PIPE).stdout.decode().strip()
    platform_string += ' OAR_JOB_ID: {}'.format(
            oar_jid if len(oar_jid) else 'None')
    platform_string += f' System: {platform.system()} {platform.version()}'
    return platform_string


def set_dd(d, key, value, sep='.', soft=False):
    """Dynamic assignment to nested dictionary
    http://stackoverflow.com/questions/21297475/set-a-value-deep-in-a-dict-dynamically"""
    dd = d
    keys = key.split(sep)
    latest = keys.pop()
    for k in keys:
        dd = dd.setdefault(k, {})
    if soft:
        dd.setdefault(latest, value)
    else:
        dd[latest] = value


def gir_merge_dicts(user, default):
    """Girschik's dict merge from F-RCNN python implementation"""
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in user:
                user[k] = v
            else:
                user[k] = gir_merge_dicts(user[k], v)
    return user


def flatten_nested_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def unflatten_nested_dict(flat_dict, sep='.'):
    nested = {}
    for k, v in flat_dict.items():
        set_dd(nested, k, v, sep)
    return nested


def indent_mstring(string, indent=4):
    """Indent multiline string"""
    return '\n'.join(map(lambda x: ' '*indent+x, string.split('\n')))


def enumerate_mstring(string, indent=4):
    estring = []
    splitted = string.split('\n')
    maxlen = math.floor(math.log(len(splitted), 10))+1
    for ind, line in enumerate(splitted):
        estring.append('{0:{1}d}{2}{3}'.format(
            ind+1, maxlen, ' '*indent, line))
    return '\n'.join(estring)


def force_symlink(path, linkname, where):
    """
    Force symlink creation. If symlink to wrong place - fail

    Important to be careful when resolving relative paths
    """
    link_fullpath = path/linkname
    where_fullpath = path/where
    if link_fullpath.is_symlink():
        r_link = link_fullpath.resolve()
        r_where = where_fullpath.resolve()
        assert r_link == r_where, \
                ('Symlink exists, but points to wrong '
                'place {} instead of {}').format(r_link, r_where)
    else:
        for i in range(256):
            try:
                link_fullpath.symlink_to(where)
                break
            except (FileExistsError, FileNotFoundError) as e:
                log.debug('Try {}: Caught {}, trying again'.format(i, e))
            finally:
                log.debug('Managed at try {}'.format(i))


def get_work_subfolder(
        workfolder,
        subfolder,
        allowed_work_subfolders=['out', 'vis', 'temp', 'log']):
    """ Check if allowed name, create if missing """

    if str(subfolder) not in allowed_work_subfolders:
        raise ValueError('Subfolder not allowed {}'.format(subfolder))
    subfolder_path = workfolder/subfolder
    return small.mkdir(subfolder_path)


def get_work_subfolders(workfolder):
    out = get_work_subfolder(workfolder, 'out')
    temp = get_work_subfolder(workfolder, 'temp')
    return out, temp
