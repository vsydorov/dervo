"""
Tools related to experiment organization (mostly procedural)
"""
import os.path
import inspect
import sys
import logging
from pathlib import Path

import yaml

import vst
from vst.exp import (
        resolve_clean_exp_path, add_logging_filehandlers,
        extend_path_reload_modules, import_routine,
        remove_first_loghandler_before_handling_error)

from dervo.snippets import (force_symlink)
from dervo.config import (build_config_yml_py)
from dervo.checkout import (get_commit_sha_repo, manage_code_checkout)

log = logging.getLogger(__name__)


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
    force_symlink(path, symlink_name, symlink_path)


def manage_workfolder(path, ycfg, co_commit_sha):
    # If separate output disabled - output goes to a subfolder
    if ycfg['_experiment']['output']['enable']:
        # Create and symlink outputfolder
        outputfolder = get_outputfolder_given_path(
            path, Path(ycfg['_experiment']['output']['dervo_root']),
            Path(ycfg['_experiment']['output']['store_root']))
        create_symlink_to_outputfolder(outputfolder, path,
                ycfg['_experiment']['output']['sl_relative'],
                ycfg['_experiment']['output']['sl_prefix'])
    else:
        outputfolder = vst.mkdir(path/'_workfolder')
    # Workfolder - specified by commit
    workfolder = vst.mkdir(outputfolder/co_commit_sha)
    return workfolder


def dump_dervo_stats(workfolder, path,
        run_string, lctr, logfilename_debug, logfilename_info):
    log.info(inspect.cleandoc(
        f"""Initialized the logging system!
        Platform: \t\t{vst.platform_info()}
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


def run_experiment(path, co_commit, add_args, fake):
    """
    Execute the Dervo experiment. Folder structure defines the experiment
    Args:
        - 'path' points to an experiment folder.
        - 'co_commit' if not RAW - check out and run that commit
        - 'add_args' are passed additionally to experiment
        - 'fake' - do not execute the experiment
    """
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
    logfilename_debug, logfilename_info = add_logging_filehandlers(workfolder)
    run_string = ycfg['_experiment']['run']
    dump_dervo_stats(workfolder, path,
        run_string, lctr, logfilename_debug, logfilename_info)

    # Establish code root (clone if necessary)
    log.info('-- { Code checkout')
    actual_code_root = manage_code_checkout(
            repo, co_commit_sha, workfolder, code_root,
            ycfg['_experiment']['checkout']['root'],
            ycfg['_experiment']['checkout']['to_workfolder'],
            ycfg['_experiment']['checkout']['post_cmd'])
    log.info(f'Actual code root: {actual_code_root}')
    log.info('-- } Code checkout')
    if repo is not None:
        repo.close()

    # Save configuration to the output folder
    str_cfg = yaml.dump(ycfg, default_flow_style=False)
    with (workfolder/'_final_cfg.yml').open('w') as f:
        print(str_cfg, file=f)
    log.debug(f'Final config:\n{str_cfg}')

    if fake:
        return

    # Deal with imports
    extend_path_reload_modules(actual_code_root)
    experiment_routine = import_routine(run_string)
    del ycfg['_experiment']  # Strip '_experiment meta' from ycfg
    log.info('- { Execute experiment routine')
    try:
        experiment_routine(workfolder, ycfg, add_args)
    except Exception as err:
        remove_first_loghandler_before_handling_error(err)
    log.info('- } Execute experiment routine')
