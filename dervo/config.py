import copy
import os.path
import pprint
import logging
import itertools
from typing import (  # NOQA
        Union, Any, NamedTuple, List, Tuple, Callable, TypeVar, Iterator,
        Iterable, Sequence)
from pathlib import Path

import yaml  # type: ignore

import vst

from dervo import snippets

log = logging.getLogger(__name__)

DEFAULT_DERVO_YML_CFG = 'dervo.yml'
DEFAULT_ROOT = '_ROOT'  # Snake looks for ROOT

DEFAULT_YML_CFG = 'cfg.yml'
DEFAULT_SNAKE_STOPPER_YML_CFG = '_ROOT_YML_CFG'

DEFAULT_PY_CFG = 'cfg.py'
DEFAULT_SNAKE_STOPPER_PY_CFG = '_ROOT_PY_CFG'

DERVO_CFG_DEFAULTS = """
# Where the heavy outputs will be stored
output_root: ~

# Where to checkout code to
checkout_root: ~

# Project from which we launch experiments
code_root: ~

# Run "make" when checking out
make: False

# Make symlinks relative (good for portability)
relative_symlinks: False

symlink_prefix: ''

# prefix to add to 'run' field when executing experiment code
code_import_prefix: 'pose3d.experiments'

# prefix to add to <meta_run> argument when executing meta_experiment code
meta_code_import_prefix: 'pose3d.experiments.meta'

# Experiment function to be executed
run: 'empty_run'
"""

# / Snake  <HHHHHHHH(:)-<

# Snake goes [deepest] --> [shallowest]
Snake = List[Tuple[Path, List[str]]]


def snake_create(path, stop_filename) -> Snake:
    """
    - Starting from 'path' ascend the filesystem tree
      - Stop when we hit a folder containing 'stop_filename'
    - Record directory contents into a Snake
    """
    snake = []  # type: List[Tuple[Path, List[str]]]
    for dir in itertools.chain([path], path.parents):
        files = [x.name for x in dir.iterdir()]
        snake.append((dir, files))
        if (stop_filename in files):
            break
    return snake


def snake_stop(snake: Snake, snake_stoppper: str):
    """
    Limit the snake configuration (cut off the head)
    - Stop ascending if stopper encountered
    """
    stopped_snake: Snake = []
    for path, files in snake:
        stopped_snake.append((path, files))
        if snake_stoppper in files:
            log.info('Snake stopped at {} because stopper {} found'.format(
                path, snake_stoppper))
            break
    return stopped_snake


def snake_match(
        snake: Snake, match: str, reverse=True):
    """Get matching filenames from the snake"""
    filepaths = [path/match for path, files in snake if match in files]
    if reverse:
        filepaths = filepaths[::-1]
    return filepaths


# YML configurations (nested dicts)


class UniqueKeyLoader(yaml.SafeLoader):
    # https://gist.github.com/pypt/94d747fe5180851196eb#gistcomment-3401011
    def construct_mapping(self, node, deep=False):
        mapping = []
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            assert key not in mapping, f'Duplicate key ("{key}") in YAML'
            mapping.append(key)
        return super().construct_mapping(node, deep)


def yml_load(f):
    # We disallow duplicate keys
    cfg = yaml.load(f, UniqueKeyLoader)
    cfg = {} if cfg is None else cfg
    return cfg


def yml_from_file(filepath: Path):
    try:
        with filepath.open('r') as f:
            return yml_load(f)
    except Exception as e:
        log.info(f'Could not load yml at {filepath}')
        raise e


def yml_list_merge(cfgs):
    merged_cfg = {}
    for cfg in cfgs:
        merged_cfg = snippets.gir_merge_dicts(copy.deepcopy(cfg), merged_cfg)
    return merged_cfg


def merged_yml_from_paths(yml_paths: Iterator[Path]):
    """
    Reads yml files, merges them.
    Returns {} if iterator is empty
    """
    merged_cfg = None
    for filepath in yml_paths:
        cfg = yml_from_file(filepath)
        merged_cfg = snippets.gir_merge_dicts(cfg, merged_cfg)
    if merged_cfg is None:
        merged_cfg = {}
    return merged_cfg


# Rest


def get_workfolder_given_path(path, root_dervo, output_root):
    """Create output folder, create symlink to it """
    # Create output folder (name defined by relative path wrt root_dervo)
    output_foldername = str(path.relative_to(root_dervo)).replace('/', '.')
    workfolder = vst.mkdir(output_root/output_foldername)
    return workfolder


def cfg_replace_prefix(cfg, root_dervo, PREFIX='DERVO@ROOT'):
    # A hacky thing that replaces @DERVO_ROOT prefix with root_dervo
    cf = vst.exp.flatten_nested_dict(cfg, '', '.')
    updates_to_make = {}
    for k, v in list(cf.items()):
        if isinstance(v, str) and v.startswith(PREFIX):
            updates_to_make[k] = os.path.abspath(v.replace(
                PREFIX, str(root_dervo.resolve())))
    if len(updates_to_make):
        log.info('Dervo prefix replacements:\n{}'.format(
            pprint.pformat(updates_to_make)))
        cf.update(updates_to_make)
        cfg = vst.exp.unflatten_nested_dict(cf)
    return cfg


def establish_dervo_configuration(path: Path):
    """
    Define dervo configuration
    - Where to save outputs, which code to access, etc
    """
    path = path.resolve()
    snake: Snake = snake_create(path, DEFAULT_ROOT)
    root_dervo: Path = snake[-1][0]  # @ROOT w.r.t dervo was launched

    yml_paths = snake_match(snake, DEFAULT_DERVO_YML_CFG)
    cfgs = [yml_from_file(f) for f in yml_paths]
    cfgs = [yml_load(DERVO_CFG_DEFAULTS), ] + cfgs
    dervo_cfg = yml_list_merge(cfgs)
    dervo_cfg = cfg_replace_prefix(dervo_cfg, root_dervo)

    workfolder = get_workfolder_given_path(
            path, root_dervo, Path(dervo_cfg['output_root']))
    return snake, dervo_cfg, workfolder, root_dervo
