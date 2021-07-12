import os
import re
import copy
import pprint
import logging
import itertools
from typing import (List, Tuple)
from pathlib import Path

from vst.exp import (
        flatten_nested_dict, set_dd, gir_merge_dicts,
        yml_load, yml_from_file)

log = logging.getLogger(__name__)


# YML configuration helpers

# / Snake  [deepest]  <HHHHHHHH(:)-<  [shallowest]
Snake = List[Tuple[Path, List[str]]]
FILENAME_ROOT = 'root_cfg.yml'  # Snake stops after seeing ROOT


def snake_create(path, stop_filename=FILENAME_ROOT) -> Snake:
    """
    - Starting from 'path' ascend the filesystem tree
      - Stop when we hit a folder containing 'stop_filename'
    - Record directory contents into a Snake
    """
    snake = []  # type: List[Tuple[Path, List[str]]]
    for fold in itertools.chain([path], path.parents):
        files = [x.name for x in fold.iterdir()]
        snake.append((fold, files))
        if (stop_filename in files):
            break
    return snake


def snake_match(
        snake: Snake, match: str, reverse=True):
    """Get matching filenames from the snake"""
    filepaths = [path/match for path, files in snake if match in files]
    if reverse:
        filepaths = filepaths[::-1]
    return filepaths


def yml_list_merge(cfgs):
    merged_cfg = {}
    for cfg in cfgs:
        merged_cfg = gir_merge_dicts(copy.deepcopy(cfg), merged_cfg)
    return merged_cfg


# Building YML + Python configuration


FILENAME_YML = 'cfg.yml'
FILENAME_PY = 'cfg.py'
DERVO_CFG_DEFAULTS = """
_experiment:
    run: ~                  # Experiment in the <module>:<function> format
    code_root: ~            # Code to checkout, import

    output:
        enable: True        # Save outputs to a different folder?
        dervo_root: ~       # Root, wrt which we compute output name
        store_root: ~       # Where the heavy outputs will be stored
        sl_relative: True   # Make symlinks relative (good for portability)
        sl_prefix: 'LINK_'  # Add this prefix to symlink names

    checkout:
        root: ~                 # Folder where we checkout code versions
        to_workfolder: False    # Put code in the workfolder
        post_cmd: ~             # Execute after checkout (for example "make")
"""

TEMPLATE_PYEVAL = '(PY|py)@(.+)'


def _load_yml_check_pyeval(snake: Snake, snake_subfolds):
    """
    Load YML scripts and check for possible pyeval queries
    """
    # Load YML files
    yml_configs = {}
    for fold, files in snake[::-1]:
        lvl = snake_subfolds.index(fold)
        cfgs = []
        for cfg_name in ['root_cfg.yml', 'cfg.yml']:
            if cfg_name in files:
                cfgs.append(yml_from_file(fold/cfg_name))
        if len(cfgs):
            yml_configs[lvl] = yml_list_merge(cfgs)

    # Check for possibly pyeval queries
    pyeval_queries = {}
    for lvl, yml_config in yml_configs.items():
        cf = flatten_nested_dict(yml_config, '', '.')
        queries = {}
        for dot_key, value in cf.items():
            if not isinstance(value, str):
                continue
            match = re.match(TEMPLATE_PYEVAL, value)
            if match:
                queries[dot_key] = match.group(2)
        if len(queries):
            pyeval_queries[lvl] = queries
    return yml_configs, pyeval_queries


def _perform_pyeval_updates(
        snake: Snake, snake_subfolds, pyeval_queries, yml_configs):
    """
    Obtain values for pyeval queries and update yml configs

    - Find .py files in the snake hierarchy
    - Create a python scope, populate with pyeval_scripts
    - Concatenate .py files, pyeval queries into a script, eval in that scope
    - Retrieve pyeval updates
    - Update values of yml configs
    """
    if not len(pyeval_queries):
        return

    log.info('-- {{ PYEVAL updates for YML files')
    # Load standalone python files
    py_filepaths = snake_match(snake, FILENAME_PY)
    pyfiles = {}
    for path in py_filepaths:
        lvl = snake_subfolds.index((path.parent))
        pyfiles[lvl] = path
    log.debug('pyeval queries\n{}'.format(pyeval_queries))
    log.debug('pyfiles per scope:\n{}'.format(pprint.pformat(pyfiles)))

    # Create python script
    cfg_script = "from pathlib import Path\nimport os\n_DRV_PYEVAL = {}\n"
    for lvl, subfold in enumerate(snake_subfolds):
        lvl_script = ""
        if lvl in pyfiles:
            filepath = pyfiles[lvl]
            lvl_script += f'# {str(filepath)}\n'
            with filepath.open() as f:
                lvl_script += f.read()
        if lvl in pyeval_queries:
            updates = pyeval_queries[lvl]
            lvl_script += f"_DRV_PYEVAL[{lvl}] = {{\n"
            for k, v in updates.items():
                lvl_script += f"  '{k}': {v},\n"
            lvl_script += '}\n'
        if len(lvl_script):
            lvl_script = (f'\n# lvl={lvl}\n'
                f'_FOLD = Path("{subfold}"); os.chdir(_FOLD)\n') + lvl_script
            cfg_script += lvl_script
    cfg_script = '#'*25+'\n'+cfg_script+'#'*25
    log.info(f'Prepared python script:\n{cfg_script}')

    # / Evaluate python script in a separate scope
    cfg_scope = {}
    from dervo import pyeval_scripts  # Here to avoid circular import
    # cfg_scope.update(pyeval_scripts.__dict__)
    cfg_scope.update({'grab': pyeval_scripts.grab})
    code = compile(cfg_script, '<cfg_script>', 'exec')
    # This allows inspecting code in PUDB
    import linecache
    linecache.cache['<cfg_script>'] = (
            len(cfg_script), None, cfg_script, '<cfg_script>')
    cwd_before = os.getcwd()
    exec(code, cfg_scope)
    os.chdir(cwd_before)  # Restore path to what it was

    # Retrieve evaluated pyeval values, replace them in YML configs
    pyeval_updates = cfg_scope['_DRV_PYEVAL']
    for lvl, updates in pyeval_updates.items():
        yml_config = yml_configs[lvl]
        for dot_key, value in updates.items():
            set_dd(yml_config, dot_key, value)
    # Display replaced values:
    pyeval_str = "Pyeval values obtained:\n"
    for lvl, updates in pyeval_updates.items():
        pyeval_str += "lvl: {} fold: {}\n".format(lvl, snake_subfolds[lvl])
        for dot_key, value in updates.items():
            pyeval_str += "  {}: {} -> {}\n".format(
                    dot_key, pyeval_queries[lvl][dot_key], value)
    log.debug(pyeval_str)
    log.info('-- }} PYEVAL updates for YML files')


def _yml_merge_summary(yml_merged_config, yml_configs, snake_subfolds):
    flat0 = flatten_nested_dict(yml_merged_config, '', '.')

    # Record level to display later
    importlevel = {}
    for level, yml_config in yml_configs.items():
        flat = flatten_nested_dict(yml_config, '', '.')
        for k, v in flat.items():
            importlevel[k] = level

    # Fancy display
    mkey = max(map(lambda x: len(x), flat0.keys()), default=0)
    mlevel = max(importlevel.values(), default=0)
    row_format = "{:<%d} {:<5} {:<%d} {:}" % (mkey, mlevel)
    output = ''
    output += "YML configurations:\n"
    for lvl, yml_config in yml_configs.items():
        if lvl >= 0:
            output += "lvl: {} fold: {}\n".format(lvl, snake_subfolds[lvl])
    output += row_format.format('key', 'source', '', 'value')+'\n'
    output += row_format.format('--', '--', '--', '--')+'\n'
    for key, value in flat0.items():
        level = importlevel.get(key, '?')
        levelstars = '*'*importlevel.get(key, 0)
        output += row_format.format(
                key, level, levelstars, value)+'\n'
    return output


def build_config_yml_py(path):
    """
    Pick up .yml and .py files, evaluate, build final nested config
    """
    log.info('- { GET_CFG: Parse experiment configuration')
    snake: Snake = snake_create(path)
    snake_subfolds = [x[0] for x in snake][::-1]
    # Load YML files, Check for possibly pyyeval queries
    yml_configs, pyeval_queries = _load_yml_check_pyeval(snake, snake_subfolds)
    # Perform pyeval_queries (if any)
    _perform_pyeval_updates(snake, snake_subfolds, pyeval_queries, yml_configs)
    # Add default configs at -1 level, resort
    yml_configs[-1] = yml_load(DERVO_CFG_DEFAULTS)
    yml_configs = dict(sorted(yml_configs.items()))
    # Merge YML configs
    yml_merged_config = yml_list_merge(yml_configs.values())
    log.info(_yml_merge_summary(
        yml_merged_config, yml_configs, snake_subfolds))
    log.info('- } GET_CFG: Parse experiment configuration')
    return yml_merged_config
