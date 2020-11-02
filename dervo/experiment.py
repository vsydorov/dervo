"""
Tools related to experiment organization (mostly procedural)
"""
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
from vst import small

from dervo import snippets

log = logging.getLogger(__name__)

DEFAULT_YML_CFG = 'cfg.yml'
# If encountered - cfg.yml snake stops
DEFAULT_SNAKE_STOPPER_YML_CFG = '_ROOT_YML_CFG'

DEFAULT_PY_CFG = 'cfg.py'
# If encountered - cfg.py snake stops
DEFAULT_SNAKE_STOPPER_PY_CFG = '_ROOT_PY_CFG'

DEFAULT_DERVO_YML_CFG = 'dervo.yml'
DEFAULT_ROOT = '_ROOT'  # Snake looks for ROOT

EXPERIMENT_PATH = None

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

# // Experimental tools
# //// Snakes and other tools for wandering around filesystem


def paternal_snake_query(path, filename) -> List[Tuple[Path, List[str]]]:
    """
    Starting for 'path' we ascend the filesystem tree until we hit a folder
    containing 'filename' or filesystem root. We record directory contents.
    Snake goes [deepest] --> [shallowest]
    """
    snake = []  # type: List[Tuple[Path, List[str]]]
    for dir in itertools.chain([path], path.parents):
        files = [x.name for x in dir.iterdir()]
        snake.append((dir, files))
        if (filename in files):
            break
    return snake


def stop_snake(
        snake: List[Tuple[Path, List[str]]],
        snake_stoppper: str):
    stopped_snake = []
    for path, files in snake:
        stopped_snake.append((path, files))
        if snake_stoppper in files:
            log.info('Snake stopped at {} because stopper {} found'.format(
                path, snake_stoppper))
            break
    return stopped_snake


def get_matching_filenames_from_snake(
        snake: List[Tuple[Path, List[str]]],
        filename: str):
    return [path/filename
            for path, files in snake
            if filename in files]


def get_nested_cfg_from_snake(
        snake: List[Tuple[Path, List[str]]],
        cfgname: str):
    """
    (YML) Search snake for 'cfgname' and do nested merge of the results
    """
    yml_filenames_ordered = get_matching_filenames_from_snake(
            snake, cfgname)[::-1]  # Deepest to shallowest
    return merge_yml_orderly(yml_filenames_ordered), yml_filenames_ordered


def merge_yml_orderly(
        yml_filenames_ordered: Iterator[Path]):
    """Returns {} if iterator is empty"""
    merged_cfg = None
    for filename in yml_filenames_ordered:
        with filename.open('r') as f:
            cfg = yaml.safe_load(f)
        # Empty file ->  empty dicts, not "None"
        cfg = {} if cfg is None else cfg
        merged_cfg = snippets.gir_merge_dicts(cfg, merged_cfg)
    if merged_cfg is None:
        merged_cfg = {}
    return merged_cfg


def get_workfolder_given_path(path, root_local, output_root):
    """Create output folder, create symlink to it """
    path = Path(path)

    # Create output folder (name defined by relative path wrt root_local)
    output_foldername = str(path.relative_to(root_local)).replace('/', '.')
    workfolder = small.mkdir(Path(output_root)/output_foldername)
    return workfolder, root_local


def prepare_updates_to_yml_given_py(cfg, py_hierarchy, snake_head):
    """
    Executes scripts defined in 'py_hierarchy' in local scope. Searches cfg for
    'PY_TEMPLATE' matches, replaces the values with local scope ones

    When executing py_hierarchy adds path information (utilizing py_hierarchy
    and snake_head)
    """
    PY_TEMPLATE = '(PY|py)@(.+)'

    # Prepare script
    script = "from pathlib import Path\n"
    for lvl, python_file in enumerate(py_hierarchy):
        with python_file.open() as f:
            # script += '\n'
            script += f'# LVL {lvl}\n'
            script += f'path = Path("{python_file.parent}")\n'
            # script += f'path = Path("{python_file.parent}")  # LVL {lvl}\n'
            script += f.read()
    # script += f'\nexp_path = Path("{snake_head}")  # Experiment folder\n'
    script += '# Experiment folder\n'
    script += f'exp_path = Path("{snake_head}")\n'

    code = compile(script, '<cfg_script>', 'exec')

    pretty_script = snippets.indent_mstring('#'*25+'\n'+script+'#'*25, 2)
    log.info(f'Executing following preparation script:\n{pretty_script}')
    log.info('--- {{{ EXEC py_code')
    try:
        exec(code, globals())  # Potentially horrible things happen
    except Exception:
        log.info('Error while executing:\n{}'.format(
            snippets.enumerate_mstring(script)))
        raise
    finally:
        log.info('--- }}} EXEC py_code')

    # Finding missing values in CFG and replacing with local scope variables
    cf = snippets.flatten_nested_dict(cfg, '', '.')
    updates_to_eval = {}
    for dot_key, value in cf.items():
        if not isinstance(value, str):
            continue
        match_ = re.match(PY_TEMPLATE, value)
        if match_:
            expr = match_.group(2)
            updates_to_eval[dot_key] = expr

    py_updates = {}
    if len(updates_to_eval):
        updates_to_eval_str = '\n'.join(f'{dot_key} <-- {expr}'
            for dot_key, expr in updates_to_eval.items())
        log.info('Such values will be evald (N={}):\n{}'.format(
            len(updates_to_eval), updates_to_eval_str))
        log.info('--- {{{ EXEC py_updates')
        for dot_key, expr in updates_to_eval.items():
            py_updates[dot_key] = eval(expr)
        log.info('--- }}} EXEC py_updates')
        py_updates_str = '\n'.join(f'{dot_key} <-- {value}'
            for dot_key, value in py_updates.items())
        log.info('Values after eval (N={}):\n{}'.format(
            len(updates_to_eval), py_updates_str))
    return py_updates


def get_configuration_yml_given_snake(
        snake: List[Tuple[Path, List[str]]],
            ):
    """
    Reconstruct part of configuration by processing only yml files
    """
    # Load YML
    cfg, yml_filenames = get_nested_cfg_from_snake(snake, DEFAULT_YML_CFG)
    log.info('Such yml configs were merged:')
    for level, path in enumerate(yml_filenames):
        log.info('  {} - {}'.format(level, path))

    log.debug('Partial config (only YML):\n{}'.format(
        snippets.indent_mstring(pprint.pformat(cfg), 4)))

    return cfg


def get_configuration_yml_output_given_snake(
        snake: List[Tuple[Path, List[str]]],
            ):
    """
    Reconstruct part of configuration by processing only yml files
    """
    # Load YML
    cfg, yml_filenames = get_nested_cfg_from_snake(snake, DEFAULT_YML_CFG)
    return cfg['outputs']


def get_configuration_py_yml_given_snake(
        snake: List[Tuple[Path, List[str]]],
            ):
    """
    Reconstruct whole configuration by processing yml, py file
    """
    # Load YML
    log.info('-- {{ Merge YML configurations')
    yml_cfg_snake = stop_snake(snake, DEFAULT_SNAKE_STOPPER_YML_CFG)
    cfg, yml_filenames = get_nested_cfg_from_snake(
            yml_cfg_snake, DEFAULT_YML_CFG)
    YML_MERGE_LEVELS = '\n'.join([
        f'{level}\t{path}'
        for level, path in enumerate(yml_filenames)])
    if len(YML_MERGE_LEVELS):
        log.info(f'Such yml configs were merged:\n{YML_MERGE_LEVELS}')
    else:
        log.info('No yml configs were merged')
    YML_AFTER_MERGE = yaml.dump(cfg, default_flow_style=False).rstrip()
    log.debug(f'YML after merge:\n{YML_AFTER_MERGE}')
    log.info('-- }} Merge YML configurations')

    # {{ PY updates to YML
    log.info('-- {{ Merge PY code, Update YML with PY')
    py_cfg_snake = stop_snake(snake, DEFAULT_SNAKE_STOPPER_PY_CFG)
    py_filenames = get_matching_filenames_from_snake(
            py_cfg_snake, DEFAULT_PY_CFG)[::-1]
    PY_MERGE_LEVELS = '\n'.join([
        f'{level}\t{path}'
        for level, path in enumerate(py_filenames)])
    log.info(f'Such .py scripts will be run:\n{PY_MERGE_LEVELS}')

    # Sample updates (python code eval inside)
    updates_to_make = prepare_updates_to_yml_given_py(
            cfg, py_filenames, snake[0][0])
    for dot_key, value in updates_to_make.items():
        snippets.set_dd(cfg, dot_key, value)
    # }} PY updates to YML
    log.info('-- }} Merge PY code, Update YML with PY')

    log.info('Merge summary:\n{}'.format(
        snippets.indent_mstring(print_flat_cfg_and_levels(
            cfg, yml_filenames, py_filenames), 2)))
    log.debug('Full config (YML+PY):\n{}'.format(snippets.indent_mstring(
        yaml.dump(cfg, default_flow_style=False).rstrip(), 2)))
    return cfg


# //// Launching experiments themselves


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


def co_repo_create(repo, co_repo_fold, co_commit_sha, run_make):
    # Create nice repo folder
    small.mkdir(co_repo_fold)
    repo.git.clone('--recursive', '--shared', '.', co_repo_fold)
    # Checkout proper commit
    co_repo = git.Repo(str(co_repo_fold))
    co_repo.git.checkout(co_commit_sha)
    co_repo.close()
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
        output_prefix = co_commit_sha
    else:
        log.info('Running raw code')
        actual_code_root = code_root
        output_prefix = 'RAW'
    return actual_code_root, output_prefix


def _manage_code_checkout(dervo_root, dervo_cfg, co_commit):
    # // Managing code (wrt git commits), obtaining well formed prefix
    log.info('-- {{ Code checkout')
    code_root = Path(dervo_cfg['code_root'])
    log.info(f'Code root: {code_root}')
    repo, commit_sha, dirty = git_repo_query(code_root)

    checkout_root = dervo_cfg['checkout_root']
    if checkout_root is None:
        checkout_root = dervo_root/'temp'
        log.warning('Checkout root not specified, using dervo temp')
    else:
        checkout_root = Path(checkout_root)

    actual_code_root, output_prefix = decide_actual_code_root(
            checkout_root, code_root, co_commit, repo,
            commit_sha, dirty, dervo_cfg['make'])
    if repo is not None:
        repo.close()
    log.info('-- }} Code checkout')
    return actual_code_root, output_prefix


def _establish_dervo_configuration(path):
    # // Define dervo configuration
    # Where to save outputs, which code to access, etc
    cfg_snake = paternal_snake_query(path, DEFAULT_ROOT)
    dervo_cfg, meta_filenames = get_nested_cfg_from_snake(
            cfg_snake, DEFAULT_DERVO_YML_CFG)
    dervo_cfg = snippets.cfg_inherit_defaults(DERVO_CFG_DEFAULTS, dervo_cfg)

    # Find @ROOT w.r.t dervo was launched
    long_snake = paternal_snake_query(path, DEFAULT_ROOT)
    root_local: Path = long_snake[-1][0]
    # Replace relative roots
    for k, v in list(dervo_cfg.items()):
        if '_root' in k and v.startswith('@ROOT'):
            dervo_cfg[k] = os.path.abspath(v.replace(
                '@ROOT', str(root_local.resolve())))

    workfolder, root_local = get_workfolder_given_path(
            path, root_local, dervo_cfg['output_root'])
    return cfg_snake, dervo_cfg, workfolder, root_local


def get_module_experiment_str(
        run: str, import_prefix: str) -> Tuple[str, str]:
    run_split = run.split('.')  # Code to be run
    if import_prefix == '':
        import_prefix_split = []  # type: ignore
    else:
        import_prefix_split = import_prefix.split('.')
    module_str = '.'.join(import_prefix_split + run_split[:-1])
    experiment_str = run_split[-1]
    return module_str, experiment_str


def _handle_experiment_error(err):
    # Remove first handler(StreamHandler to stderr) to avoid double clutter
    our_logger = logging.getLogger()
    assert len(our_logger.handlers), \
            'Logger handlers are empty for some reason'
    if isinstance(our_logger.handlers[0], logging.StreamHandler):
        our_logger.removeHandler(our_logger.handlers[0])
    log.exception("Fatal error in experiment routine")
    raise err


def run_experiment(dervo_root, path, add_args, co_commit: str = None):
    """
    Executes the Dervo experiment
    Args:
        - 'dervo_root' points to root directory of dervo
        - 'path' points to an experiment cfg folder.
            - Folder structure found defines experiment
        - 'add_args' are passed additionally to experiment
        - 'co_commit' if not None will check out a specific version of code and
          operate on that code
    """
    cfg_snake, dervo_cfg, workfolder, root_local = \
            _establish_dervo_configuration(path)

    # Vital symlink logic
    if dervo_cfg['relative_symlinks']:
        symlink_path = Path(os.path.relpath(workfolder, path))
    else:
        symlink_path = Path(workfolder)
    symlink_name = dervo_cfg['symlink_prefix']+workfolder.name
    snippets.force_symlink(path, symlink_name, symlink_path)

    assert isinstance(logging.getLogger().handlers[0],
            logging.StreamHandler), 'First handler should be StreamHandler'

    with small.LogCaptorToRecords(pause_others=True) as lctr:
        actual_code_root, output_prefix = _manage_code_checkout(
                dervo_root, dervo_cfg, co_commit)

    # Cleanly separate outputs per commit sha
    prefixed_workfolder = small.mkdir(workfolder/output_prefix)

    # Find proper experiment routine
    module_str, experiment_str = get_module_experiment_str(
            dervo_cfg['run'], dervo_cfg['code_import_prefix'])

    # Set up logging
    logfolder = snippets.get_work_subfolder(prefixed_workfolder, 'log')
    id_string = small.get_experiment_id_string()
    logfilename_debug = small.add_filehandler(
            logfolder/f'{id_string}.DEBUG.log', logging.DEBUG, 'extended')
    logfilename_info = small.add_filehandler(
            logfolder/f'{id_string}.INFO.log', logging.INFO, 'short')
    log.info(inspect.cleandoc(
        f"""Welcome to the logging system!
        Platform: \t\t{snippets.platform_info()}
        Debug file: \t\t{logfilename_debug}
        Info file: \t\t{logfilename_info}
        Experiment path: \t{path}
        Workfolder path: \t{prefixed_workfolder}
        Root (local): \t\t{root_local}
        Actual code root: \t{actual_code_root}
        --- Python --
        VENV:\t\t\t{small.is_venv()}
        Prefix:\t\t\t{sys.prefix}
        --- Code ---
        Module: \t\t{module_str}
        Experiment: \t\t{experiment_str}
        """))
    from pip._internal.operations import freeze
    log.debug('pip freeze: {}'.format(';'.join(freeze.freeze())))

    log.info('- { CAPTURED: Loglines before system init')
    lctr.handle_captured()
    log.info('- } CAPTURED: Loglines before system init')

    # Whole configuration reconstructed here
    log.info('- { GET_CFG: Parse experiment configuration')
    cfg = get_configuration_py_yml_given_snake(cfg_snake)
    log.info('- } GET_CFG: Parse experiment configuration')

    # Save final config to the output folder
    # TODO: Warn if changed from last time
    with (prefixed_workfolder/'final_config.cfg').open('w') as f:
        print(yaml.dump(cfg, default_flow_style=False), file=f)

    # Extend pythonpath to allow importing certain modules
    sys.path.insert(0, str(actual_code_root))

    # Unload caches, to allow local version (if present) to take over
    importlib.invalidate_caches()
    import vst
    importlib.reload(vst)
    importlib.reload(small)
    importlib.reload(vst.plot)

    # Import experiment routine
    module = importlib.import_module(module_str)
    experiment_routine = getattr(module, experiment_str)

    # I'll cheat and create global variable here
    global EXPERIMENT_PATH
    EXPERIMENT_PATH = path

    # Execute experiment routine
    try:
        experiment_routine(prefixed_workfolder, cfg, add_args)
    except Exception as err:
        _handle_experiment_error(err)


def run_meta_experiment(dervo_root, path, meta_run,
        add_args, co_commit: str = None):
    """
    Executes the Dervo META experiment (EXPERIMENTAL)
        META experiment is executed on top of existing experiments and
        provides useful utilities like visualization

    Args:
        - 'dervo_root' points to root directory of dervo
        - 'path' points to an experiment cfg folder.
            - Folder structure found defines experiment
        - 'meta_run' meta-function to be run on top of experiment
        - 'add_args' are passed additionally to experiment
        - 'co_commit' if not None will check out a specific version of code and
          operate on that code
    """
    raise NotImplementedError('Meta experiment unsupported for now')

    # # // Find out about experiment we are running on top of
    # log.info('Running a META Experiment')
    # cfg_snake, dervo_cfg, workfolder, root_local = \
    #         _establish_dervo_configuration(path)
    # snippets.force_symlink(path/workfolder.name, workfolder)
    #
    # # Capture loglevel equal to first handler (should be streamhandler)
    # first_handler = logging.getLogger().handlers[0]
    # assert isinstance(first_handler, logging.StreamHandler), \
    #         'First handler should be StreamHandler'
    # with small.LoggingCapturer(first_handler.level) as lcap:
    #     actual_code_root, output_prefix = _manage_code_checkout(
    #             dervo_root, dervo_cfg, co_commit)
    #
    # # Cleanly separate outputs per commit sha
    # prefixed_workfolder = small.mkdir(workfolder/output_prefix)
    #
    # # Set up logging (put 'meta' logging into the 'log/meta')
    # logfolder = small.mkdir(small.get_work_subfolder(
    #         prefixed_workfolder, 'log')/'meta')
    # logfilename = organize.set_filehandler_given_folder_and_time(logfolder)
    # log.info('-- {{ META Experiment info --')
    # log.info(f'Started logging into {logfilename}')
    # log.info(f'Meta Experiment (on top) of path: {path}')
    # log.info(f'Workfolder path: {prefixed_workfolder}')
    # log.info(f'Root (local): {root_local}')
    # log.info(f'Actual code root: {actual_code_root}')
    # log.info(f'Checkout info:\n{lcap.captured.strip()}')
    # log.info(organize.platform_info())
    # log.info('-- }} META Experiment info --')
    #
    # # Whole configuration reconstructed here
    # log.info('-- {{ Obtaining experiment configuration --')
    # cfg = get_configuration_py_yml_given_snake(cfg_snake)
    # log.info('-- }} Obtaining experiment configuration --')
    #
    # # Extend pythonpath to allow importing certain modules
    # sys.path.append(str(actual_code_root))
    #
    # # // Find proper routines (experiment and meta-experiment)
    # # What routine does this experiment normally run?
    # module_experiment_str = '.'.join(get_module_experiment_str(
    #     dervo_cfg['run'], dervo_cfg['code_import_prefix']))
    # # What meta routine should we run
    # meta_module_str, meta_experiment_str = get_module_experiment_str(meta_run,
    #         dervo_cfg['meta_code_import_prefix'])
    #
    # # Import metaexperiment routine
    # meta_module = importlib.import_module(meta_module_str)
    # meta_experiment_routine = getattr(meta_module, meta_experiment_str)
    #
    # # Execute metaexperiment routine
    # try:
    #     meta_experiment_routine(prefixed_workfolder,
    #             cfg, module_experiment_str, add_args)
    # except Exception as err:
    #     _handle_experiment_error(err)


# // GLUE
# Glue functions are allowed to be called by cfg.py files


def grab(
        path: Union[Path, str],
        rel_path: str,
        must_exist=True) -> str:
    """
    Glue. Find absolute path to workfolder of another experiment, append
    rel_path to it, makes sure file exists.
    """
    log.info(f'<<< BEGIN GLUE (grab). Grab: {rel_path} @ {path}')
    # Dervo configuration allows us to look up workfolder
    with vst.logging_disabled(logging.INFO):
        cfg_snake, dervo_cfg, workfolder, root_local = \
                _establish_dervo_configuration(Path(path))
    # Try to find requested path
    item_to_find = workfolder/rel_path
    if must_exist and not item_to_find.exists():
        raise FileNotFoundError(f'Could not grab from {item_to_find}')
    log.info('>>> END GLUE (grab). Grabbed {}'.format(item_to_find))
    return str(item_to_find)


def get_tags(path):
    """
    Glue. Finds all tagged folders (tag must be unique), adds them to dict
    """
    # NFS stupidity prevention
    while True:
        try:
            taglist = [[x.name, x.parent] for x in path.glob('**/T@*')]
            break
        except (OSError) as e:
            log.debug('Caught {}, trying again'.format(e))
    tags = dict(taglist)
    if len(tags) != len(taglist):
        log.error("You've got non-unique tags!")
        log.error('\n'.join(map(
            lambda x: "{0:25} <- {1}".format(*x), taglist)))
        raise ValueError('Tags must be unique!')
    if len(taglist) > 0:
        maxlen = max(len(x[0]) for x in taglist)
        descr = '\n'.join(map(lambda x: "{:{}} <- {}".format(
            x[0], maxlen, x[1].relative_to(path)), taglist))
        log.info(f'TAGS FOUND from {path}:\n{descr}')
    else:
        log.info(f'NO TAGS FOUND from {path}')

    return tags


# // Other functions


def print_flat_cfg_and_levels(cfg, yml_hierarchy, py_hierarchy):
    flat0 = snippets.flatten_nested_dict(cfg, '', '.')

    # Record level to display later
    importlevel = {}
    for level, path in enumerate(yml_hierarchy):
        with path.open('r') as f:
            cfg_ = yaml.safe_load(f)
        # Empty file -> empty dict, not "None"
        cfg_ = {} if cfg_ is None else cfg_

        for k, v in snippets.flatten_nested_dict(cfg_, '', '.').items():
            importlevel[k] = level

    # Fancy display
    mkey = max(map(lambda x: len(x), flat0.keys()), default=0)
    mlevel = max(importlevel.values(), default=0)
    row_format = "{:<%d} {:<5} {:<%d} {:}" % (mkey, mlevel)
    output = row_format.format('key', 'source', '', 'value')
    output += '\n'+row_format.format('--', '--', '--', '--')
    for key, value in flat0.items():
        level = importlevel.get(key, '?')
        levelstars = '*'*importlevel.get(key, 0)
        output += '\n'+row_format.format(
                key, level, levelstars, value)
    return output
