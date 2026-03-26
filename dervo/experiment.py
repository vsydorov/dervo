"""
Tools related to experiment organization
"""

import copy
import os
import os.path
import inspect
import sys
import logging
import importlib
import contextlib
from typing import Dict, List, Union
from pathlib import Path

import yaml
from omegaconf import OmegaConf as OC, open_dict
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
        value: Union[str, List[str]]
        if isinstance(absolute, str):
            value = os.path.relpath(absolute, workfolder)
        elif isinstance(absolute, list):
            value = [os.path.relpath(a, workfolder) for a in absolute]
        else:
            raise RuntimeError(f"Wrong type for {absolute=} at {key=}")
        obj[parts[-1]] = value
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
    return experiment_routine, module


def remove_first_loghandler_before_handling_error(err):
    # Remove first handler(StreamHandler to stderr) to avoid double clutter
    our_logger = logging.getLogger()
    assert len(our_logger.handlers), "Logger handlers are empty for some reason"
    if isinstance(our_logger.handlers[0], logging.StreamHandler):
        our_logger.removeHandler(our_logger.handlers[0])
    log.exception("Fatal error in experiment routine")
    raise err


def get_hydra_closure_params(func) -> Dict[str, str]:
    """
    Extract hydra params from @hydra.main closure.
    Resolve config_path to absolute path
    """
    params = {}
    if not (hasattr(func, "__wrapped__") and func.__closure__ is not None):
        return params
    freevars = func.__code__.co_freevars
    cells = {}
    for name, cell in zip(freevars, func.__closure__):
        try:
            cells[name] = cell.cell_contents
        except ValueError:
            pass
    for k, v in cells.items():
        if k in ["config_path", "config_name"]:
            params[k] = v
        if k == "config_path" and not os.path.isabs(v):
            # Relative according to what we know is __wrapped__()
            module_file = inspect.getfile(func.__wrapped__)
            params[k] = os.path.join(os.path.dirname(module_file), v)
    return params


def _query_update_hydra_params(routine, module, cfg) -> Dict[str, str]:
    hydra_params = get_hydra_closure_params(routine)
    if "_hydra" in cfg:
        for k, v in cfg["_hydra"].items():
            if k in ["config_path", "config_name"]:
                hydra_params[k] = v
            if k == "config_path" and not os.path.isabs(hydra_params[k]):
                # Relative according to module (hydra closure not gauranteed)
                hydra_params[k] = os.path.join(os.path.dirname(module.__file__), v)
    return hydra_params


def _hydra_update_config(cfg_routine, workfolder, hydra_params, hydra_groups):
    """
    NOTE: This pollutes global scope with hydra stuff (GlobalHydra and HydraConfig).
      No way around it, since users of hydra sometimes access params via
      hydra.core.hydra_config.HydraConfig.get()
    """
    # Initialise hydra
    log.info(f"Preparing Hydra config.\n{hydra_params=}\n{hydra_groups=}")
    from hydra import compose, initialize_config_dir
    from hydra.core.hydra_config import HydraConfig

    # Generate hydra configuration
    initialize_config_dir(
        config_dir=hydra_params.get("config_path"),
        version_base=hydra_params.get("version_base", "1.3"),
        job_name="dervo",
    )
    hydra_groups_overrides = [f"{k}={v}" for k, v in hydra_groups.items()]
    cfg_hydra = compose(
        config_name=hydra_params["config_name"],
        overrides=hydra_groups_overrides,
        return_hydra_config=True,
    )
    # Merge-in dervo config values
    cfg_hydra = OC.merge(cfg_hydra, cfg_routine)
    # Update output_dir
    OC.update(cfg_hydra, "hydra.runtime.output_dir", str(workfolder))
    # Update the global hydra config (unfortunate necessity)
    HydraConfig().set_config(cfg_hydra)

    # Separate the internal for hydra config, dump
    with (workfolder / "CONFIG.hydra.internals.yml").open("w") as f:
        yaml.dump(
            OC.to_container(cfg_hydra.hydra, resolve=False),
            f,
            default_flow_style=False,
            sort_keys=False,
        )

    cfg_hydra = copy.deepcopy(cfg_hydra)
    with open_dict(cfg_hydra):
        del cfg_hydra["hydra"]

    with (workfolder / "CONFIG.hydra.yml").open("w") as f:
        yaml.dump(
            OC.to_container(cfg_hydra, resolve=False),
            f,
            default_flow_style=False,
            sort_keys=False,
        )
    cfg_routine = cfg_hydra
    return cfg_routine


def run_experiment(path, co_commit, add_args):
    """
    Execute the Dervo experiment. Folder structure defines the experiment
    Args:
        - 'path' points to an experiment folder.
        - 'co_commit' if not RAW - check out and run that commit
        - 'add_args' are passed additionally to experiment
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
    routine, module = import_routine(run_string)

    # Prepare config for routine to consume (without meta keys)
    keys_routine = [k for k in cfg if k not in ["_dervo", "_hydra"]]
    cfg_routine = OC.masked_copy(cfg, keys_routine)

    # Special handling for hydra experiments
    hydra_params = _query_update_hydra_params(routine, module, cfg)
    if hydra_params.get("config_name"):
        hydra_groups = cfg.get("_hydra", {}).get("groups", {})
        cfg_routine = _hydra_update_config(
            cfg_routine, workfolder, hydra_params, hydra_groups
        )
        # Try unwrapping if looks hydra-wrapped with @main decorator
        if inspect.getfile(routine).endswith("hydra/main.py"):
            routine = getattr(routine, "__wrapped__", routine)

    # Force chdir instead of passing workfolder
    log.info(f"Changing cwd to workfolder: {workfolder}")
    os.chdir(workfolder)

    # Accomodate optional add_args
    kwargs_routine = {}
    if "add_args" in inspect.signature(routine).parameters:
        kwargs_routine = {"add_args": add_args}

    log.info("- [ Execute experiment routine")
    try:
        routine(cfg_routine, **kwargs_routine)
    except Exception as err:
        remove_first_loghandler_before_handling_error(err)
