import logging
from pathlib import Path
from typing import ( # NOQA
        Dict, NamedTuple, List, TypeVar, Union, Tuple,
        Any, Callable, Iterator)

import vst

from dervo.experiment import (establish_dervo_configuration)

log = logging.getLogger(__name__)


def anygrab(
        path: Union[Path, str],
        rel_path: str = None,
        commit: str = None,
        must_exist=True) -> str:
    """
    Several modes:
    * anygrab(path):
      - grab the file at path
    * anygrab(path, rel_path, [commit]):
      - grab experiment at path, open workfolder
      - if defined, get commit subfold, otherwise first subfold
      - get rel_path
    - (optionally) makes sure file exists.
    """
    if rel_path is None:
        item_to_find = Path(path).resolve()
    else:
        # Dervo configuration allows us to look up workfolder
        with vst.logging_disabled(logging.INFO):
            snake, dervo_cfg, workfolder, root_dervo = \
                    establish_dervo_configuration(Path(path))
        # Resolve commit
        if commit is None:
            subfolders = list(workfolder.iterdir())
            if not len(subfolders):
                raise RuntimeError('Anygrab fail: no commit subfolders')
            commitfolder = subfolders[0]
        else:
            commitfolder = workfolder/commit
        # Now get the item
        item_to_find = commitfolder/rel_path

    if must_exist and not item_to_find.exists():
        raise FileNotFoundError(f'Could not grab from {item_to_find}')
    return str(item_to_find)
