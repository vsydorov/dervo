"""
Tools related to experiment organization (mostly procedural)
"""
import os.path
import inspect
import sys
import importlib
import logging
import yaml  # type: ignore
from pathlib import Path

import vst

from dervo import snippets
from dervo.config import (build_config_yml_py)
from dervo.checkout import (get_commit_sha_repo, manage_code_checkout)

log = logging.getLogger(__name__)


def resolve_clean_exp_path(path_: str) -> Path:
    path = Path(path_)
    assert path.exists(), f'Path must exists: {path}'
    if path.is_file():
        log.warning('File instead of dir was provided, using its parent instead')
        path = path.parent
    path = path.resolve()
    return path


def get_outputfolder_given_path(
        path: Path, dervo_root: Path, output_root: Path):
    """Create output folder, create symlink to it """
    # Create output folder (name defined by relative path wrt root_dervo)
    output_foldername = str(path.relative_to(dervo_root)).replace('/', '.')
    workfolder = vst.mkdir(output_root/output_foldername)
    return workfolder


def create_symlink_to_outputfolder(
        outputfolder, path, sl_relative, sl_prefix):
    if sl_relative:
        symlink_path = Path(os.path.relpath(outputfolder, path))
    else:
        symlink_path = Path(outputfolder)
    symlink_name = sl_prefix+outputfolder.name
    snippets.force_symlink(path, symlink_name, symlink_path)


def manage_workfolder(path, ycfg, co_commit_sha):
    # If separate output disabled - output to path
    if not ycfg['_experiment']['output']['enable']:
        return path
    # Create and symlink outputfolder
    outputfolder = get_outputfolder_given_path(
        path, Path(ycfg['_experiment']['output']['dervo_root']),
        Path(ycfg['_experiment']['output']['store_root']))
    create_symlink_to_outputfolder(outputfolder, path,
            ycfg['_experiment']['output']['sl_relative'],
            ycfg['_experiment']['output']['sl_prefix'])
    # Workfolder - specified by commit
    workfolder = vst.mkdir(outputfolder/co_commit_sha)
    return workfolder


def setup_logging(workfolder):
    # Create two output files in /log subfolder, start loggign
    assert isinstance(logging.getLogger().handlers[0],
            logging.StreamHandler), 'First handler should be StreamHandler'
    logfolder = vst.mkdir(workfolder/'_log')
    id_string = vst.get_experiment_id_string()
    logfilename_debug = vst.add_filehandler(
            logfolder/f'{id_string}.DEBUG.log', logging.DEBUG, 'extended')
    logfilename_info = vst.add_filehandler(
            logfolder/f'{id_string}.INFO.log', logging.INFO, 'short')
    return logfilename_debug, logfilename_info


def dump_dervo_stats(workfolder, path,
        run_string, lctr, logfilename_debug, logfilename_info):
    log.info(inspect.cleandoc(
        f"""Initialized the logging system!
        Platform: \t\t{snippets.platform_info()}
        Experiment path: \t{path}
        Workfolder path: \t{workfolder}
        --- Python --
        VENV:\t\t\t{vst.is_venv()}
        Prefix:\t\t\t{sys.prefix}
        --- Code ---
        Experiment: \t\t{run_string}
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


def extend_path_reload_modules(actual_code_root):
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


def import_routine(run_string):
    module_str, experiment_str = run_string.split(':')
    module = importlib.import_module(module_str)
    experiment_routine = getattr(module, experiment_str)
    return experiment_routine


def handle_experiment_error(err):
    # Remove first handler(StreamHandler to stderr) to avoid double clutter
    our_logger = logging.getLogger()
    assert len(our_logger.handlers), \
            'Logger handlers are empty for some reason'
    if isinstance(our_logger.handlers[0], logging.StreamHandler):
        our_logger.removeHandler(our_logger.handlers[0])
    log.exception("Fatal error in experiment routine")
    raise err


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
    log.info('|||-------------------------------------------------------|||')
    log.info('    Start of Dervo experiment')
    # Capture logs, before we establish location for logfiles
    with vst.LogCaptorToRecords(pause_others=True) as lctr:
        path = resolve_clean_exp_path(path)
        # Establish configuration
        ycfg = build_config_yml_py(path)
        # Establish workfolder
        code_root = ycfg['_experiment']['code_root']
        assert code_root is not None, 'code_root should be set'
        code_root = Path(code_root)
        co_commit_sha, repo = get_commit_sha_repo(code_root, co_commit)
        workfolder = manage_workfolder(path, ycfg, co_commit_sha)

    # Setup logging in the workfolder
    logfilename_debug, logfilename_info = setup_logging(workfolder)
    run_string = ycfg['_experiment']['run']
    dump_dervo_stats(workfolder, path,
        run_string, lctr, logfilename_debug, logfilename_info)

    # Establish code root (clone if necessary)
    log.info('-- {{ Code checkout')
    actual_code_root = manage_code_checkout(
            repo, co_commit_sha, workfolder, code_root,
            ycfg['_experiment']['checkout']['root'],
            ycfg['_experiment']['checkout']['to_workfolder'],
            ycfg['_experiment']['checkout']['post_cmd'])
    log.info(f'Actual code root: {actual_code_root}')
    log.info('-- }} Code checkout')
    if repo is not None:
        repo.close()

    # Save configuration to the output folder
    str_cfg = yaml.dump(ycfg, default_flow_style=False)
    with (workfolder/'root_cfg.yml').open('w') as f:
        print(str_cfg, file=f)
    log.debug(f'Final config:\n{str_cfg}')

    # Deal with imports
    extend_path_reload_modules(actual_code_root)
    experiment_routine = import_routine(run_string)
    del ycfg['_experiment']  # Strip '_experiment meta' from ycfg
    log.info('- { GET_CFG: Execute experiment routine')
    try:
        experiment_routine(workfolder, ycfg, add_args)
    except Exception as err:
        handle_experiment_error(err)
    log.info('- } GET_CFG: Execute experiment routine')
    log.info('    End of Dervo experiment')
    log.info('|||-------------------------------------------------------|||')
