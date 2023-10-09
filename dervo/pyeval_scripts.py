import os.path
from os.path import (normpath, join)
import logging
from pathlib import Path
from typing import ( # NOQA
        Dict, NamedTuple, List, TypeVar, Union, Tuple,
        Any, Callable, Iterator)

from dervo.experiment import (
        get_outputfolder_given_path,)
from dervo.config import (
        build_config_yml_py)

log = logging.getLogger(__name__)


def get_outputfolder_via_dervo(path):
    """Dervo configuration allows us to look up workfolder"""
    log.info(f'%%% Grabbing via dervo from {str(path)} %%%')
    # Limit allowed pyeval keys to those necessary to infer workfolder
    ycfg = build_config_yml_py(path, [
        '_experiment.output.enable',
        '_experiment.output.dervo_root',
        '_experiment.output.store_root'])
    # If separate output disabled - output goes to a subfolder
    if ycfg['_experiment']['output']['enable']:
        outputfolder = get_outputfolder_given_path(
            path, Path(ycfg['_experiment']['output']['dervo_root']),
            Path(ycfg['_experiment']['output']['store_root']))
    else:
        outputfolder = path/'_workfolder'
    return outputfolder


def get_outputfolder_via_guess(path):
    """
    Try to guess where dervo.experiment.manage_workfolder puts outputs
    - Find folder named _outputfolder
    or
    - Follow the longest symlink in the folder
    """
    log.info(f'%%% Grabbing via guess from {str(path)} %%%')
    if (outputfolder := path/'_outputfolder').exists():
        return outputfolder
    symlinks = [str(x) for x in path.iterdir() if os.path.islink(x)]
    if not len(symlinks):
        raise RuntimeError('No _outputfolder and no symlinks! Could not guess outputfolder')
    longest = Path(max(symlinks, key=len))
    outputfolder = longest.resolve()
    log.info('Resolved:\nFound {}\nvia {}'.format(str(outputfolder), longest.name))
    return Path(outputfolder)


def grab(
        path: Union[Path, str],
        rel_path: str = None,
        commit: str = None,
        must_exist=True,
        outputfolder_via='guess') -> str:
    """
    Several modes:
    * grab(path):
      - grab the file at path
    * grab(path, rel_path, [commit]):
      - grab experiment at path, open workfolder
      - if defined, get commit subfold, otherwise first subfold
      - get rel_path
    - (optionally) makes sure file exists.
    """
    #  normalize without resolving symlinks
    path = Path(normpath(join(os.getcwd(), path)))

    if rel_path is None:
        item_to_find = path
    else:
        if outputfolder_via == 'dervo':
            outputfolder = get_outputfolder_via_dervo(path)
        elif outputfolder_via == 'guess':
            outputfolder = get_outputfolder_via_guess(path)
        else:
            raise RuntimeError(f'Wrong {outputfolder_via=}')

        if commit is None:
            # Take earliest created folder
            subfolders = list(outputfolder.iterdir())
            if not len(subfolders):
                raise RuntimeError('Grab fail: no commit subfolders')
            workfolder = max(subfolders, key=lambda x: x.stat().st_ctime)
        else:
            workfolder = outputfolder/commit

        # Now get the item
        item_to_find = workfolder/rel_path

    if must_exist and not item_to_find.exists():
        raise FileNotFoundError(f'Grab fail: file missing at {item_to_find}')
    return str(item_to_find)


PYEVAL_SCOPE = {'grab': grab}
