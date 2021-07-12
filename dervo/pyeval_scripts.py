import os.path
from os.path import (normpath, join)
import logging
from pathlib import Path
from typing import ( # NOQA
        Dict, NamedTuple, List, TypeVar, Union, Tuple,
        Any, Callable, Iterator)

import vst

from dervo.experiment import (
        get_outputfolder_given_path,)
from dervo.checkout import (
        get_commit_sha_repo)
from dervo.config import (
        build_config_yml_py)

log = logging.getLogger(__name__)


def grab(
        path: Union[Path, str],
        rel_path: str = None,
        commit: str = None,
        must_exist=True) -> str:
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
    # / normalize without resolving symlinks
    # TODO: avoid relying on getcwd/chdir. Nested "grab" calls don't play nicely
    # path = Path(os.path.abspath(path))
    path = Path(normpath(join(os.getcwd(), path)))
    if rel_path is None:
        item_to_find = path
    else:
        # Dervo configuration allows us to look up workfolder
        with vst.logging_disabled(logging.INFO):
            ycfg = build_config_yml_py(path)
            # If separate output disabled - output goes to a subfolder
            if ycfg['_experiment']['output']['enable']:
                outputfolder = get_outputfolder_given_path(
                    path, Path(ycfg['_experiment']['output']['dervo_root']),
                    Path(ycfg['_experiment']['output']['store_root']))
            else:
                outputfolder = path/'_workfolder'
            # Resolve commit
            if commit is None:
                subfolders = list(outputfolder.iterdir())
                if not len(subfolders):
                    raise RuntimeError('Grab fail: no commit subfolders')
                workfolder = subfolders[0]
            else:
                workfolder = outputfolder/commit
        # Now get the item
        item_to_find = workfolder/rel_path

    if must_exist and not item_to_find.exists():
        raise FileNotFoundError(f'Could not grab from {item_to_find}')
    return str(item_to_find)
