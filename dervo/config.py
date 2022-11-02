import os
import re
import copy
import pprint
import logging
import itertools
from functools import partial
from typing import (Dict, List, Tuple)
from pathlib import Path

import vst
from vst.exp import (
        flatten_nested_dict, set_dd, gir_merge_dicts,
        yml_load, yml_from_file)

log = logging.getLogger(__name__)

pf = partial(pprint.pformat, sort_dicts=False, width=200)


# Hardcoded filenames
FILENAME_ROOT = 'root_cfg.yml'  # Snake stops after seeing ROOT
FILENAME_YML = 'cfg.yml'
FILENAME_PY = 'cfg.py'

# Template for PYEVAL (trigger python evaluation inside YML)
TEMPLATE_PYEVAL = '(PY|py)@(.+)'

# YML configuration helpers

"""
Snake: sequence of folder ancestors with directory contents
"""
Snake = Dict[Path, List[str]]


def snake_create(path, stop_filename=FILENAME_ROOT) -> Snake:
    """
    - Starting from 'path' ascend the filesystem tree
      - [deepest]  <HHHHHHHH(:)-<  [shallowest]
      - Stop when we hit a folder containing 'stop_filename'
    - Record directory contents
    - Reverse to [shallowest] -> [deepest]
    """
    snake_lst = []
    for fold in itertools.chain([path], path.parents):
        files = [x.name for x in fold.iterdir()]
        snake_lst.append((fold, files))
        if (stop_filename in files):
            break
    return dict(snake_lst[::-1])


def yml_list_merge(cfgs):
    merged_cfg = {}
    for cfg in cfgs:
        merged_cfg = gir_merge_dicts(copy.deepcopy(cfg), merged_cfg)
    return merged_cfg


# Building YML + Python configuration
DERVO_CFG_DEFAULTS = """
_experiment:
    run: ~                  # Experiment in the <module>:<function> format
    code_root: ~            # Code to checkout, import
    commit: 'RAW'           # Default commit to execute

    output:
        enable: True        # Save outputs to a different folder?
        dervo_root: ~       # Root, wrt which we compute output name
        store_root: ~       # Where the heavy outputs will be stored
        sl_relative: True   # Make symlinks relative (good for portability)
        sl_prefix: 'LINK_'  # Add this prefix to symlink names

    checkout:
        root: ~                  # Folder where we checkout code versions
        to_workfolder: False     # Put code in the workfolder
        post_cmd: ~              # Execute after checkout (for example "make")
        local_submodules: False  # Manually clone submodules, instead of
                                 #   checking out from URLs (faster, does not
                                 #   require internet, but can be buggy)
"""


def _load_yml_from_snake(
        snake: Snake, filenames_cfg=[FILENAME_ROOT, FILENAME_YML]
        ) -> Dict[int, Dict]:
    """
    Load YML config files from snake
    """
    yml_configs = {}
    for lvl, (fold, filenames) in enumerate(snake.items()):
        cfgs = {filename: yml_from_file(fold/filename)
                for filename in (set(filenames_cfg) & set(filenames))}
        if len(cfgs):
            yml_configs[lvl] = yml_list_merge(cfgs.values())
    yml_str = '\n--- {{{ YML configs per folder\n'
    for lvl, cfg in yml_configs.items():
        path = str(list(snake)[lvl])
        yml_str += ('== lvl: {} {} ==\n{}\n'.format(lvl, path, pf(cfg)))
    yml_str += '--- }}} YML configs per folder'
    log.debug(yml_str)
    return yml_configs


def _find_pyeval_queries(
        yml_configs: Dict[int, Dict],
        pyeval_allowed_keys=None,
        template_pyeval=TEMPLATE_PYEVAL):
    """
    Extract PYEVAL queries from yml configs, optionally filter them
    """
    # Check for possible pyeval queries
    pyeval_queries = {}
    for lvl, yml_config in yml_configs.items():
        cf = flatten_nested_dict(yml_config, '', '.')
        queries = {}
        for dot_key, value in cf.items():
            if not isinstance(value, str):
                continue
            if match := re.match(template_pyeval, value):
                queries[dot_key] = match.group(2)
        if len(queries):
            pyeval_queries[lvl] = queries

    # Limit pyeval queries to allowed subset
    if pyeval_allowed_keys is not None:
        pyeval_queries_allowed = {}
        for lvl, querydict in pyeval_queries.items():
            keys = set(querydict.keys()) & set(pyeval_allowed_keys)
            querydict_allowed = {k: querydict[k] for k in keys}
            if len(querydict_allowed):
                pyeval_queries_allowed[lvl] = querydict_allowed
        pyeval_queries = pyeval_queries_allowed

    if len(pyeval_queries):
        pyq_str = '\n--- {{{ Extracted PYEVAL queries:\n'
        if pyeval_allowed_keys is not None:
            pyq_str += f'{pyeval_allowed_keys=}\n'
        pyq_str += pf(pyeval_queries) + '\n'
        pyq_str += '--- }}} Extracted PYEVAL queries'
        log.debug(pyq_str)
    return pyeval_queries


def mprefix(s, prefix):
    return '\n'.join(map(lambda x: f'{prefix}{x}', s.split('\n')))


def _perform_pyeval_updates(
        snake: Snake, pyeval_queries, yml_configs):
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

    log.info('--- {{{ PYEVAL updates for YML files')
    # Load standalone python files
    pyfiles = {}
    for lvl, (fold, files) in enumerate(snake.items()):
        if FILENAME_PY in files:
            pyfiles[lvl] = fold/FILENAME_PY
    log.debug('pyfiles per scope:\n{}'.format(pf(pyfiles)))

    # Create python script
    cfg_script = "from pathlib import Path\nimport os\n_DRV_PYEVAL = {}\n"
    for lvl, (fold, _) in enumerate(snake.items()):
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
                f'_FOLD = Path("{fold}"); os.chdir(_FOLD)\n') + lvl_script
            cfg_script += lvl_script
    cfg_script = '#'*25+'\n'+cfg_script+'#'*25
    log.info(f'Prepared python script:\n{cfg_script}')

    # / Evaluate python script in a separate scope
    cfg_scope = {}
    from dervo import pyeval_scripts  # Here to avoid circular import
    cfg_scope.update(pyeval_scripts.PYEVAL_SCOPE)
    code = compile(cfg_script, '<cfg_script>', 'exec')
    # This allows inspecting code in PUDB
    import linecache
    linecache.cache['<cfg_script>'] = (
            len(cfg_script), None, cfg_script, '<cfg_script>')
    cwd_before = os.getcwd()
    with vst.LogCaptorToRecords(pause_others=True) as lctr:
        exec(code, cfg_scope)
    # Manually print all logging outputs from within <cfg_script>
    if len(lctr.captured) > 0:
        captured_str = '---- {{{{ CAPTURED_PYEVAL\n'
        for line in lctr.captured:
            captured_str += mprefix(
                    f'{line.name} {line.levelname}: '
                    + line.getMessage(), '>> ') + '\n'
        captured_str +='---- }}}} CAPTURED_PYEVAL'
        log.debug(captured_str)
    os.chdir(cwd_before)  # Restore path to what it was

    # Retrieve evaluated pyeval values, replace them in YML configs
    pyeval_updates = cfg_scope['_DRV_PYEVAL']
    for lvl, updates in pyeval_updates.items():
        yml_config = yml_configs[lvl]
        for dot_key, value in updates.items():
            set_dd(yml_config, dot_key, value)
    # Display replaced values:
    pyeval_str = "---- {{{{ PYEVAL_REPLACED:\n"
    for lvl, updates in pyeval_updates.items():
        pyeval_str += "== lvl: {} {} ==\n".format(lvl, list(snake)[lvl])
        for dot_key, value in updates.items():
            pyeval_str += "  + {}:\n    FROM: {}\n    TO: {}\n".format(
                    dot_key, pyeval_queries[lvl][dot_key], value)
    pyeval_str += "---- }}}} PYEVAL_REPLACED"
    log.debug(pyeval_str)
    log.info('--- }}} PYEVAL updates for YML files')


def _yml_merge_summary(yml_merged_config, yml_configs, snake: Snake):
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
            output += "lvl: {} fold: {}\n".format(lvl, list(snake)[lvl])
    output += row_format.format('key', 'source', '', 'value')+'\n'
    output += row_format.format('--', '--', '--', '--')+'\n'
    for key, value in flat0.items():
        level = importlevel.get(key, '?')
        levelstars = '*'*importlevel.get(key, 0)
        output += row_format.format(
                key, level, levelstars, value)+'\n'
    return output


def build_config_yml_py(path, pyeval_allowed_keys=None):
    """
    Pick up .yml and .py files, evaluate, build final nested config
    """
    log.info('-- {{ GET_CFG: Parse experiment configuration')
    snake: Snake = snake_create(path)

    yml_configs = _load_yml_from_snake(snake)
    pyeval_queries = _find_pyeval_queries(yml_configs, pyeval_allowed_keys)
    _perform_pyeval_updates(snake, pyeval_queries, yml_configs)
    # Add default configs at -1 level, resort
    yml_configs[-1] = yml_load(DERVO_CFG_DEFAULTS)
    yml_configs = dict(sorted(yml_configs.items()))
    # Merge YML configs
    yml_merged_config = yml_list_merge(yml_configs.values())
    log.info(_yml_merge_summary(yml_merged_config, yml_configs, snake))
    log.info('-- }} GET_CFG: Parse experiment configuration')
    return yml_merged_config
