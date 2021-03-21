#!/usr/bin/env python3
"""
Interactive tool for creation/removal of multiple subfolders. This is useful
for gridsearches and like.

Usage:
    run_grid.py <path>
    run_grid.py <path> (remove|purge) [--symlinks]

Options:
    --symlinks      Follow symlinks and remove output dirs too

File format:
    <path> should contain a grid.yml file of following structure:
        vars:
            <NAME>: <V>
            ...
    Depending on <V> type different stuff happens:
        - list, we take values from list
        - string, we eval(V)
        - int, we take range(V)
"""
import shutil
import pprint
import yaml
import logging
import numpy as np  # type: ignore
import itertools
from pathlib import Path
from docopt import docopt  # type: ignore

import vst

from dervo import experiment, snippets

DEFAULT_GRID_CFG = 'grid.yml'

log = logging.getLogger(__name__)


def _content_match(file, file_contents):
    """Makes sure that file contains certain text"""
    with file.open('r') as f:
        real_contents = f.read()
    return real_contents == file_contents


def resolve_endnode_vars(var):
    if type(var) is list:
        all_values = var
    elif type(var) is str:
        all_values = eval(var)
    elif type(var) is int:
        all_values = list(range(var))
    else:
        raise NotImplementedError(f'Unknown type for variable {var}')
    return all_values


def resolve_node(k, v):
    if k == 'ALIGN':
        child_kvs = [resolve_node(k, v) for k, v in v.items()]
        assert len(set([len(x) for x in child_kvs])) == 1, \
            f'align children must be same size,\n{child_kvs}'
        key_values = []
        for x in zip(*child_kvs):
            key_values.append(dict(z for y in x for z in y.items()))
    elif k == 'CROSS':
        child_kvs = [resolve_node(k, v) for k, v in v.items()]
        key_values = []
        for x in itertools.product(*child_kvs):
            key_values.append(dict(z for y in x for z in y.items()))
    else:
        values = resolve_endnode_vars(v)
        python_vars = [np.asscalar(v)
            if isinstance(v, np.generic) else v for v in values]
        key_values = [{k: v} for v in python_vars]
    return key_values


class GridManager(object):
    def __init__(self, path):
        self.path = snippets.find_exp_path(path)
        self.description_path = self.path/DEFAULT_GRID_CFG
        if not self.description_path.exists():
            raise ValueError('No grid description found at {}'.format(
                self.description_path))
        self.build_grid_structure()

    def build_grid_structure(self):
        with open(self.description_path, 'r') as f:
            descr = yaml.safe_load(f)

        # Variables and their values (sorted)
        self.key_values = []
        for k, v in descr.items():
            self.key_values.extend(resolve_node(k, v))

        # Folders and cfg values
        self.folds_and_cfgs = []
        for ind, kvdict in enumerate(self.key_values):
            foldname = 'grid:{}:'.format(':'.join([
                f'{k}={v}' for k, v in kvdict.items()]))
            if ' ' in foldname:
                log.warning('Spaces in folder name. Trying to fix')
                foldname = foldname.replace(' ', '')
            file_contents = yaml.dump(
                    vst.exp.unflatten_nested_dict(kvdict))
            self.folds_and_cfgs.append((foldname, file_contents, kvdict))

    @staticmethod
    def folder_match(cfg_fold, file_contents):
        """Makes sure that folder contains config file and (optionally)
        symlinks"""
        cfg_file = cfg_fold/experiment.DEFAULT_YML_CFG
        if not cfg_file.exists():
            return False, 'cfg_file not existing'
        if not _content_match(cfg_file, file_contents):
            return False, 'cfg_file content mismatch'
        for it in cfg_fold.iterdir():
            if it.is_symlink() or it == cfg_file:
                continue
            else:
                return False, 'cfg_fold contains extra file'
        return True, None

    def install_folders(self):
        N = 0
        for foldname, file_contents, _ in self.folds_and_cfgs:
            cfg_fold = self.path/foldname
            cfg_fold.mkdir(exist_ok=True)
            cfg_file = cfg_fold/experiment.DEFAULT_YML_CFG
            if cfg_file.exists():
                if not _content_match(cfg_file, file_contents):
                    log.warning('Grid file contents do not match '
                            'what must be inside')
            else:
                with cfg_file.open('w') as f:
                    print(file_contents, file=f, end='')
                N += 1
        log.info(f'Created {N} cfgs')

    def remove_folders(self):
        N = 0
        for foldname, file_contents, _ in self.folds_and_cfgs:
            fold = self.path/foldname
            if fold.exists():
                match, reason = self.folder_match(fold, file_contents)
                if match:
                    for file in fold.iterdir():
                        file.unlink()
                    fold.rmdir()
                    N += 1
                else:
                    log.warning(f'Unable to delete {fold} because {reason}')
        log.info(f'Removed {N} folders')

    def purge_folders(self):
        N = 0
        to_purge_folders = []
        to_purge_files = []
        for file in self.path.glob('*'):
            if file.name in (
                    experiment.DEFAULT_YML_CFG,
                    experiment.DEFAULT_PY_CFG,
                    experiment.DEFAULT_DERVO_YML_CFG,
                    DEFAULT_GRID_CFG):
                continue
            else:
                if file.is_dir():
                    to_purge_folders.append(file)
                else:
                    to_purge_files.append(file)
        print('Remove folders:\n' +
            '\n'.join(map(str, to_purge_folders)),
                '\nRemove files:\n' +
            '\n'.join(map(str, to_purge_files)))
        s = input('Remove surely (Y/N)? --> ')
        if s in ('Y', 'y'):
            for file in to_purge_files:
                file.unlink()
                N += 1
            for file in to_purge_folders:
                shutil.rmtree(file)
                N += 1
            log.info(f'Purged {N} files/folders')

    def purge_symlinks(self):
        N = 0
        to_purge_symlinks = []
        to_purge_real_files = []
        for file in self.path.rglob('*'):
            if file.is_symlink():
                to_purge_symlinks.append(file)
                to_purge_real_files.append(file.resolve())
        print('Remove folders:\n' +
            '\n'.join(map(str, to_purge_symlinks)),
                '\nRemove files:\n' +
            '\n'.join(map(str, to_purge_real_files)))
        s = input('Remove surely (Y/N)? --> ')
        if s in ('Y', 'y'):
            for file in to_purge_symlinks:
                file.unlink()
                N += 1
            for file in to_purge_real_files:
                shutil.rmtree(file)
                N += 1
            log.info(f'Purged {N} files/folders')


def main(args):
    path = Path(args['<path>'])
    grid = GridManager(path)
    if args.get('remove') or args.get('purge'):
        if args['--symlinks']:
            grid.purge_symlinks()
        if args.get('remove'):
            grid.remove_folders()
        elif args.get('purge'):
            grid.purge_folders()
    else:
        log.info('Values are {}'.format(pprint.pformat(grid.key_values)))
        grid.install_folders()


if __name__ == '__main__':
    args = docopt(__doc__)
    log = vst.reasonable_logging_setup(logging.INFO)
    main(args)
