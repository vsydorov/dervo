"""
Tools related to experiment organization (mostly procedural)
"""
import copy
import os.path
import inspect
import subprocess
import sys
import tempfile
import time
import git  # type: ignore
import re
import importlib
import logging
import pprint
import yaml  # type: ignore
import itertools
from datetime import datetime
from pathlib import Path
from typing import (Dict, NamedTuple, List, TypeVar, Union, Tuple, # NOQA
        Any, Callable, Iterator)  # NOQA

import vst
from vst.exp import (unflatten_nested_dict, flatten_nested_dict, set_dd)

from dervo import snippets
from dervo.config import (Snake, snake_stop, snake_match,
        DEFAULT_SNAKE_STOPPER_PY_CFG, DEFAULT_PY_CFG,
        DEFAULT_SNAKE_STOPPER_YML_CFG, DEFAULT_YML_CFG,
        establish_dervo_configuration, cfg_replace_prefix,
        yml_list_merge, yml_from_file)

log = logging.getLogger(__name__)

TEMPLATE_PYEVAL = '(PY|py)@(.+)'

EXPERIMENT_PATH = None


# Experiment boilerplate


def get_module_experiment_str(
        run: str, import_prefix: str) -> Tuple[str, str]:
    if len(import_prefix):
        run = import_prefix.strip('.') + '.' + run
    dot_split = run.split('.')
    module_str = '.'.join(dot_split[:-1])
    experiment_str = dot_split[-1]
    return module_str, experiment_str


def create_symlink_to_workfolder(workfolder, path,
        relative_symlinks, symlink_prefix):
    if relative_symlinks:
        symlink_path = Path(os.path.relpath(workfolder, path))
    else:
        symlink_path = Path(workfolder)
    symlink_name = symlink_prefix+workfolder.name
    snippets.force_symlink(path, symlink_name, symlink_path)


def setup_logging(
        workfolder_w_commit, path, root_dervo, actual_code_root,
        module_str, experiment_str, lctr):
    # Create two output files in /log subfolder, start loggign
    assert isinstance(logging.getLogger().handlers[0],
            logging.StreamHandler), 'First handler should be StreamHandler'
    logfolder = vst.mkdir(workfolder_w_commit/'log')
    id_string = vst.get_experiment_id_string()
    logfilename_debug = vst.add_filehandler(
            logfolder/f'{id_string}.DEBUG.log', logging.DEBUG, 'extended')
    logfilename_info = vst.add_filehandler(
            logfolder/f'{id_string}.INFO.log', logging.INFO, 'short')
    log.info(inspect.cleandoc(
        f"""Initialized the logging system!
        Platform: \t\t{snippets.platform_info()}
        Experiment path: \t{path}
        Workfolder path: \t{workfolder_w_commit}
        Dervo root: \t\t{root_dervo}
        Actual code root: \t{actual_code_root}
        --- Python --
        VENV:\t\t\t{vst.is_venv()}
        Prefix:\t\t\t{sys.prefix}
        --- Code ---
        Module: \t\t{module_str}
        Experiment: \t\t{experiment_str}
        -- Logging --
        DEBUG logfile: \t\t{logfilename_debug}
        INFO logfile: \t\t{logfilename_info}
        """))
    from pip._internal.operations import freeze
    log.debug('pip freeze: {}'.format(';'.join(freeze.freeze())))
    # Release previously captured logging records
    log.info('- { CAPTURED: Loglines before system init')
    lctr.handle_captured()
    log.info('- } CAPTURED: Loglines before system init')


def deal_with_imports(actual_code_root, module_str, experiment_str):
    # Extend pythonpath to allow importing certain modules
    sys.path.insert(0, str(actual_code_root))
    # Unload caches, to allow local version (if present) to take over
    importlib.invalidate_caches()
    # Reload vst and then submoduless (avoid issues with __init__ imports)
    # https://stackoverflow.com/questions/35640590/how-do-i-reload-a-python-submodule/51074507#51074507
    importlib.reload(vst)
    for k, v in list(sys.modules.items()):
        if k.startswith('vst'):
            log.debug(f'Reload {k} {v}')
            importlib.reload(v)
    # Import experiment routine
    module = importlib.import_module(module_str)
    experiment_routine = getattr(module, experiment_str)
    return module, experiment_routine


def handle_experiment_error(err):
    # Remove first handler(StreamHandler to stderr) to avoid double clutter
    our_logger = logging.getLogger()
    assert len(our_logger.handlers), \
            'Logger handlers are empty for some reason'
    if isinstance(our_logger.handlers[0], logging.StreamHandler):
        our_logger.removeHandler(our_logger.handlers[0])
    log.exception("Fatal error in experiment routine")
    raise err


# Checking out


def git_repo_query(code_root: Path) -> Tuple[git.Repo, str, bool]:
    try:
        repo = git.Repo(str(code_root))
        # Current commit info
        try:
            branch = repo.active_branch.name
        except TypeError as e:
            if repo.head.is_detached:
                branch = 'DETACHED_HEAD'
            else:
                raise e

        commit_sha = repo.head.commit.hexsha
        summary = repo.head.commit.summary
        log.info('Git repo found [branch {}, Commit {}({})]'.format(
            branch, commit_sha, summary))
        dirty = repo.is_dirty()
        if dirty:
            dirty_diff = repo.git.diff()
            log.info('Repo is dirty')
            log.debug('Dirty repo diff:\n{}'.format(dirty_diff))
    except git.exc.InvalidGitRepositoryError:
        log.info('No git repo found')
        repo, commit_sha, dirty = None, None, False

    return repo, commit_sha, dirty


def co_repo_check(co_repo_fold: Path, co_commit_sha: str):
    """ Check if repo is active. Check size of repo too """
    try:
        co_repo = git.Repo(str(co_repo_fold))
        assert co_repo.head.commit.hexsha == co_commit_sha, \
                'commit shas must be same'
        assert (co_repo_fold/'FINISHED').exists(), 'FINISHED file must exists'
        return True
    except (git.exc.NoSuchPathError,
            git.exc.InvalidGitRepositoryError, AssertionError) as e:
        log.info(f'Repo check failed, because of {e.__repr__()}')
        return False


def shared_clone(repo, rpath, co_repo_fold, commit_sha):
    # Checkout proper commit
    repo.git.clone('--shared', rpath, str(co_repo_fold))
    co_repo = git.Repo(str(co_repo_fold))
    co_repo.git.checkout(commit_sha)
    co_repo.close()


def co_repo_create(repo, co_repo_fold, co_commit_sha, run_make):
    # Create nice repo folder
    vst.mkdir(co_repo_fold)
    shared_clone(repo, '.', co_repo_fold, co_commit_sha)
    # Submodules cloned individually (avoid querying the remote)
    submodules = [repo.git.submodule('status').strip().split(' ')]
    for commit_sha, subfold, _ in submodules:
        shared_clone(repo, subfold, co_repo_fold/subfold, commit_sha)

    # Run make if Makefile exists (try several times)
    if (co_repo_fold/'Makefile').exists():
        if run_make:
            make_output = None
            for i in range(2):
                try:
                    make_output = subprocess.check_output(
                            f'cd {co_repo_fold} && make',
                            shell=True,
                            stderr=subprocess.STDOUT,
                            executable='/bin/bash').strip().decode()
                    break
                except subprocess.CalledProcessError as e:
                    log.info('({}) Waiting a bit. Caught ({}):\n{}'.format(
                        i, e, e.output.decode()))
                    time.sleep(5)
            if make_output is None:
                raise OSError('Could not execute make')
            log.info(f'Executed make at {co_repo_fold}')
            log.debug(f'Output of executed make:\n{make_output}')
        else:
            log.info('Makefile execution skipped, '
                    'due to run_make=False setting')
    # Create 'FINISHED' file to indicate that repo is ready
    (co_repo_fold/'FINISHED').touch()


def co_repo_checkout(repo, co_repo_fold, co_commit_sha, run_make):
    """
    Checkout repo carefully
    """
    if not co_repo_fold.exists():
        co_repo_create(repo, co_repo_fold, co_commit_sha, run_make)
        log.info(f'Checked out code at {co_repo_fold}')
    else:
        # If folder exists - wait a bit (maybe repo is being checked out by
        # another job)
        for i in range(2):
            co_good = co_repo_check(co_repo_fold, co_commit_sha)
            if co_good:
                break
            log.info(f'({i}) Waiting for checked out folder '
                    f'to appear at {co_repo_fold}')
            time.sleep(5)
        # If waiting did not help - create alternative folder
        if not co_good:
            datetime_now = datetime.now().strftime('%Y-%m-%d_%H-%m_')
            co_repo_fold = Path(tempfile.mkdtemp(
                    prefix=datetime_now,
                    dir=str(co_repo_fold.parent),
                    suffix='temp'))
            co_repo_create(repo, co_repo_fold, co_commit_sha, run_make)
            log.info(f'Checked out code at alternative '
                    f'location {co_repo_fold}')
    return co_repo_fold


def decide_actual_code_root(
        checkout_root: Path,
        code_root: Path,
        co_commit: str,
        repo: git.Repo,
        commit_sha: str,
        dirty: bool,
        run_make: bool) -> Tuple[Path, str]:
    """
    co_commit is None:
        - Use code_root
        - Prefix is 'RAW'
    co_commit is set:
        - Checkout
        - Make sure repo exists and is in good condition.
        - Try avoiding concurrency problems.
        - Prefix is SHA
    """
    if co_commit is not None:
        assert (repo is not None) and (commit_sha is not None)
        # We trust that co_commit is either SHA or HEAD. If 'HEAD', must not be
        # dirty
        if co_commit == 'HEAD':
            assert not dirty, ('We disallow checking out HEAD of dirty repo. '
                    'Call with "--raw" or provide commit sha')
        # Assign hexsha for commit we are trying to exract
        try:
            git_commit = repo.commit(co_commit)
            co_commit_sha = git_commit.hexsha
            log.info('Commit sha is {}'.format(co_commit_sha))
        except git.BadName as e:
            log.warning('Improper commit_sha {}'.format(co_commit))
            raise e
        # Properly unify this guy
        co_repo_basename = code_root.name
        co_repo_fold = checkout_root/f'{co_repo_basename}/{co_commit_sha}'
        co_repo_fold = co_repo_checkout(
                repo, co_repo_fold, co_commit_sha, run_make)
        actual_code_root = co_repo_fold
        commit_foldname = co_commit_sha
    else:
        log.info('Running raw code')
        actual_code_root = code_root
        commit_foldname = 'RAW'
    return actual_code_root, commit_foldname


def manage_code_checkout(dervo_cfg, co_commit):
    # // Managing code (wrt git commits), obtaining well formed prefix
    log.info('-- {{ Code checkout')
    code_root = Path(dervo_cfg['code_root'])
    log.info(f'Code root: {code_root}')
    repo, commit_sha, dirty = git_repo_query(code_root)

    checkout_root = dervo_cfg['checkout_root']
    if checkout_root is None:
        checkout_root = Path('checkout_temp').resolve()
        log.warning('Checkout root not specified -, using dervo temp')
    else:
        checkout_root = Path(checkout_root)

    actual_code_root, commit_foldname = decide_actual_code_root(
            checkout_root, code_root, co_commit, repo,
            commit_sha, dirty, dervo_cfg['make'])
    if repo is not None:
        repo.close()
    log.info('-- }} Code checkout')
    return actual_code_root, commit_foldname


# CFG/PY configuration reconstruction


def _r_load_yml_pyeval(snake, snake_subfolds, root_dervo):
    """
    Load YML scripts and check for possible pyeval queries
    """
    yml_cfg_snake = snake_stop(snake, DEFAULT_SNAKE_STOPPER_YML_CFG)
    yml_filepaths = snake_match(yml_cfg_snake, DEFAULT_YML_CFG)
    yml_configs = {}
    for path in yml_filepaths:
        lvl = snake_subfolds.index((path.parent))
        yml_config = yml_from_file(path)
        yml_config = cfg_replace_prefix(yml_config, root_dervo)
        yml_configs[lvl] = yml_config
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


def _r_perform_pyeval_updates(
        snake, snake_subfolds, pyeval_queries, yml_configs):
    """
    Obtain values for pyeval queries and update yml configs

    - Find .py files in the snake hierarchy
    - Create a python scope, populate with pyeval_scripts
    - Concatenate .py files, pyeval queries into a script, eval in that scope
    - Retrieve pyeval updates
    - Update values of yml configs
    """
    log.info('-- {{ PYEVAL updates for YML files')
    # Load standalone python files
    py_cfg_snake = snake_stop(snake, DEFAULT_SNAKE_STOPPER_PY_CFG)
    py_filepaths = snake_match(py_cfg_snake, DEFAULT_PY_CFG)
    pyfiles = {}
    for path in py_filepaths:
        lvl = snake_subfolds.index((path.parent))
        pyfiles[lvl] = path
    log.debug('pyeval queries\n{}'.format(pyeval_queries))
    log.debug('pyfiles per scope:\n{}'.format(pprint.pformat(pyfiles)))

    # Create python script
    cfg_script = "_DRV_PYEVAL = {}\n"
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
            lvl_script = f'\n# lvl={lvl}\nepath = Path("{subfold}")\n' + lvl_script
            cfg_script += lvl_script
    cfg_script = '#'*25+'\n'+cfg_script+'#'*25
    log.info(f'Prepared python script:\n{cfg_script}')

    # Evaluate python script in a separate scope
    cfg_scope = {}
    from dervo import pyeval_scripts
    cfg_scope.update(pyeval_scripts.__dict__)
    code = compile(cfg_script, '<cfg_script>', 'exec')
    exec(code, cfg_scope)

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
        output += "lvl: {} fold: {}\n".format(lvl, snake_subfolds[lvl])
    output += row_format.format('key', 'source', '', 'value')+'\n'
    output += row_format.format('--', '--', '--', '--')+'\n'
    for key, value in flat0.items():
        level = importlevel.get(key, '?')
        levelstars = '*'*importlevel.get(key, 0)
        output += row_format.format(
                key, level, levelstars, value)+'\n'
    return output


def reconstruct_experiment_config(snake: Snake):
    """
    Reconstruct whole configuration by processing .yml, .py files
    - Load .yml hierarchy
    - If "pyeval" fields (py@..) were found in .yml
      - Sequentially execute .py fields, replace pyeval fields
    - Merge .yml hierarchy
    """
    root_dervo: Path = snake[-1][0]
    snake_subfolds = [x[0] for x in snake][::-1]
    # snake_rpaths = [os.path.relpath(x, root_dervo) for x in snake_subfolds]

    # Load YML files
    yml_configs, pyeval_queries = _r_load_yml_pyeval(
            snake, snake_subfolds, root_dervo)
    # Perform pyeval_queries
    if len(pyeval_queries):
        _r_perform_pyeval_updates(
                snake, snake_subfolds, pyeval_queries, yml_configs)
    # Merge YML configs
    yml_merged_config = yml_list_merge(yml_configs.values())
    log.info(_yml_merge_summary(
        yml_merged_config, yml_configs, snake_subfolds))
    return yml_merged_config


# Experiment launch


def run_experiment(path, add_args, co_commit: str = None):
    """
    Executes the Dervo experiment
    Args:
        - 'path' points to an experiment cfg folder.
            - Folder structure found defines the experiment
        - 'add_args' are passed additionally to experiment
        - 'co_commit' if not None will check out a specific version of code and
          operate on that code
    """
    path = path.resolve()

    global EXPERIMENT_PATH
    EXPERIMENT_PATH = path

    with vst.LogCaptorToRecords(pause_others=True) as lctr:
        # Establish dervo configuration (.yml merge)
        snake, dervo_cfg, workfolder, root_dervo = \
                establish_dervo_configuration(path)
        # Module/experiment split
        module_str, experiment_str = get_module_experiment_str(
                dervo_cfg['run'], dervo_cfg['code_import_prefix'])
        # Establish code root (clone if necessary)
        actual_code_root, commit_foldname = manage_code_checkout(
                dervo_cfg, co_commit)
        # Prepare workfolder
        create_symlink_to_workfolder(workfolder, path,
                dervo_cfg['relative_symlinks'], dervo_cfg['symlink_prefix'])
        workfolder_w_commit = vst.mkdir(workfolder/commit_foldname)

    setup_logging(workfolder_w_commit, path, root_dervo,
            actual_code_root, module_str, experiment_str, lctr)

    log.info('- { GET_CFG: Parse experiment configuration')
    cfg = reconstruct_experiment_config(snake)
    # Save final config to the output folder
    str_cfg = yaml.dump(cfg, default_flow_style=False)
    log.debug(f'Final config:\n{str_cfg}')
    with (workfolder_w_commit/'final_config.cfg').open('w') as f:
        print(str_cfg, file=f)
    log.info('- } GET_CFG: Parse experiment configuration')

    module, experiment_routine = deal_with_imports(
            actual_code_root, module_str, experiment_str)

    log.info('- { GET_CFG: Execute experiment routine')
    try:
        experiment_routine(workfolder_w_commit, cfg, add_args)
    except Exception as err:
        handle_experiment_error(err)
    log.info('- } GET_CFG: Execute experiment routine')
