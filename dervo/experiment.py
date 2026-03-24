"""
Tools related to experiment organization
"""

import copy
import os.path
import inspect
import sys
import logging
import importlib
from pathlib import Path

import yaml
from omegaconf import OmegaConf as OC
from dervo.config import build_config_dag_inheritance, abspath
from dervo.git import RAWCOMMIT, get_commit_sha_repo, manage_code_checkout

import vst

log = logging.getLogger(__name__)

FOLDER_OUTPUT = "OUT"
FOLDER_LOGS = "LOGS"


def _save_relative_config(workfolder: Path, container: dict, caret_keys: dict):
    """Save config with caret_key paths expressed relative to workfolder."""
    container = copy.deepcopy(container)
    for key, absolute in caret_keys.items():
        parts = key.split(".")
        obj = container
        for part in parts[:-1]:
            obj = obj[part]
        obj[parts[-1]] = os.path.relpath(absolute, workfolder)
    with (workfolder / "CONFIG.drv.relative.yml").open("w") as f:
        yaml.dump(container, f, default_flow_style=False, sort_keys=False)


def _help_locate_config(path_: Path, priority=["cfg.yml", "config.yml"]) -> Path:
    """If dir -> Try to pick up config inside"""
    path = Path(path_)
    assert path.exists(), f"Path must exists: {path}"
    if path.is_dir():
        inferred = None
        for cfgname in priority:
            candidate = path / cfgname
            if candidate.exists() and candidate.is_file():
                inferred = candidate
                break
        if inferred is None:
            yml_files = list(path.glob("*.yml"))
            assert len(yml_files), f"Folder must include *.yml: {path}"
            inferred = yml_files[0]
        log.warning("Directory provided, inferred config file as {}".format(inferred))
        path = inferred
    return path  # Absolutise to os.getcwd(), don't resolve symlinks


def add_logging_filehandlers(workfolder):
    """Create DEBUG/INFO logging files, start logging"""
    assert isinstance(
        logging.getLogger().handlers[0], logging.StreamHandler
    ), "First handler should be StreamHandler"
    logfolder = vst.mkdir(workfolder / FOLDER_LOGS)
    id_string = vst.get_experiment_id_string()
    logfilename_debug = vst.add_filehandler(
        logfolder / f"{id_string}.DEBUG.log", logging.DEBUG, "extended"
    )
    logfilename_info = vst.add_filehandler(
        logfolder / f"{id_string}.INFO.log", logging.INFO, "short"
    )
    return logfilename_debug, logfilename_info


def dump_dervo_stats(
    workfolder, path, run_string, lctr, logfilename_debug, logfilename_info
):
    # Release previously captured logging records
    lctr.handle_captured()
    log.info(inspect.cleandoc(f"""Initialized the logging system!
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

    log.debug("pip freeze: {}".format(";".join(freeze.freeze())))


def extend_path_reload_modules(actual_code_root):
    # Extend pythonpath to allow importing
    if actual_code_root is not None:
        sys.path.insert(0, str(actual_code_root))
    # Unload caches, to allow local version (if present) to take over
    importlib.invalidate_caches()
    # Reload vst and then submoduless (avoid issues with __init__ imports)
    importlib.reload(vst)
    for k, v in list(sys.modules.items()):
        if k.startswith("vst"):
            log.debug(f"Reload {k} {v}")
            importlib.reload(v)


def import_routine(run_string):
    module_str, experiment_str = run_string.split(":")
    module = importlib.import_module(module_str)
    experiment_routine = getattr(module, experiment_str)
    return experiment_routine


def remove_first_loghandler_before_handling_error(err):
    # Remove first handler(StreamHandler to stderr) to avoid double clutter
    our_logger = logging.getLogger()
    assert len(our_logger.handlers), "Logger handlers are empty for some reason"
    if isinstance(our_logger.handlers[0], logging.StreamHandler):
        our_logger.removeHandler(our_logger.handlers[0])
    log.exception("Fatal error in experiment routine")
    raise err


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
    with vst.LogCaptorToRecords(pause="file") as lctr:
        log.info("- CAPTURING: Loglines before system init -")
        path = _help_locate_config(abspath(path))
        # Establish configuration
        cfg, caret_keys = build_config_dag_inheritance(path)
        # Establish full checkout commit sha, if not RAWCOMMIT
        code_root = cfg["_dervo"]["code"]
        assert code_root is not None, "code_root should be set"
        code_root = Path(code_root)
        if co_commit is None:
            co_commit = cfg["_dervo"].get("commit", RAWCOMMIT)
            log.info("No commit passed, setting _dervo.commit = {}".format(co_commit))
        co_commit_sha, repo = get_commit_sha_repo(code_root, co_commit)
        workfolder = vst.mkdir(path.parent / FOLDER_OUTPUT / co_commit_sha)

    # Setup logging in the workfolder
    logfilename_debug, logfilename_info = add_logging_filehandlers(workfolder)
    assert (run_string := cfg["_dervo"].get("run")), "_dervo.run must be defined"
    dump_dervo_stats(
        workfolder, path, run_string, lctr, logfilename_debug, logfilename_info
    )

    # Establish code root (clone if necessary)
    if co_commit_sha == RAWCOMMIT:
        actual_code_root = code_root
    else:
        log.info("- [ Code checkout:")
        actual_code_root = manage_code_checkout(
            repo,
            co_commit_sha,
            workfolder,
            code_root,
            cfg["_dervo"]["checkout"]["folder"],
            cfg["_dervo"]["checkout"]["post_cmd"],
            cfg["_dervo"]["checkout"]["local_submodules"],
        )
        log.info("- ] Code checkout")
    if repo is not None:
        repo.close()
    log.info(f"Actual code root: {actual_code_root}")

    # Save the resolved dervo config
    container = OC.to_container(cfg, resolve=True)
    with (workfolder / "CONFIG.drv.yml").open("w") as f:
        yaml.dump(container, f, default_flow_style=False, sort_keys=False)
    _save_relative_config(workfolder, container, caret_keys)

    # Properly import the experiment routine
    extend_path_reload_modules(actual_code_root)
    experiment_routine = import_routine(run_string)

    log.info("- [ Execute experiment routine")
    try:
        experiment_routine(workfolder, cfg, add_args)
    except Exception as err:
        remove_first_loghandler_before_handling_error(err)
    log.info("- ] Execute experiment routine")
