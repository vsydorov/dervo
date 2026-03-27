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
import platform
import random
import string
import subprocess
from datetime import datetime
from typing import Dict, List, Union
from pathlib import Path

import yaml
from omegaconf import OmegaConf as OC, open_dict
from pip._internal.operations import freeze

from dervo.config import build_config_dag_inheritance
from dervo.git import RAWCOMMIT, get_commit_sha_repo, manage_code_checkout
from dervo.logging import (
    add_logging_filehandlers,
    clamp_package_loglevels,
    LogCaptorToRecords,
)
from dervo.misc import mkdir, abspath

log = logging.getLogger(__name__)

FOLDER_OUTPUT = "OUT"
FOLDER_LOGS = "LOGS"


def is_venv():
    # https://stackoverflow.com/questions/1871549/determine-if-python-is-running-inside-virtualenv
    return hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )


def get_experiment_id_string():
    """
    Unique-ish string to indenify experiment start time
    """
    time_now = datetime.now()
    str_time = time_now.strftime("%Y-%m-%d-%H-%M-%S")
    str_ms = time_now.strftime("%f")
    str_rnd = str_ms[:3] + "".join(random.choices(string.ascii_uppercase, k=3))
    str_node = platform.node()
    return f"{str_time}_{str_rnd}_{str_node}"


def platform_info():
    platform_string = f"Node: {platform.node()}"
    platform_string += f" System: {platform.system()} {platform.version()}"
    return platform_string


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


def dump_dervo_stats(workfolder, path, run_string, logfilehandlers):
    messages = []
    messages.append("Initialized the logging system!")
    messages.append("--- Paths ---")
    messages.append(f"Experiment path:         {path}")
    messages.append(f"Workfolder path:         {workfolder}")
    for k, v in logfilehandlers.items():
        messages.append(
            "log {} ({} / {}):    {}".format(
                k,
                v["loglevel_int"],
                v["loglevel_str"],
                v["logfilepath"],
            )
        )
    messages.append("--- Platform ---")
    messages.append(f"Node:   {platform.node()}")
    messages.append(f"System: {platform.system()} / {platform.version()}")
    # todo : add slurm job id
    oar_jobid = (
        subprocess.run("echo $OAR_JOB_ID", shell=True, stdout=subprocess.PIPE)
        .stdout.decode()
        .strip()
    )
    oar_jobid = oar_jobid if len(oar_jobid) else "None"
    messages.append(f"OAR_JOB_ID: {oar_jobid}")

    messages.append("--- Python ---")
    messages.append(f"VENV:    {is_venv()}")
    messages.append(f"Prefix:    {sys.prefix}")

    messages.append("--- Code ---")
    messages.append(f"Experiment:    {run_string}")

    log.info("\n".join(messages))
    log.debug("pip freeze: {}".format(";".join(freeze.freeze())))


def extend_path_reload_modules(actual_code_root):
    # Extend pythonpath to allow importing
    if actual_code_root is not None:
        sys.path.insert(0, str(actual_code_root))
    # Unload caches, to allow local version (if present) to take over
    importlib.invalidate_caches()
    # # Reload vst and then submoduless (avoid issues with __init__ imports)
    # importlib.reload(vst)
    # for k, v in list(sys.modules.items()):
    #     if k.startswith("vst"):
    #         log.debug(f"Reload {k} {v}")
    #         importlib.reload(v)


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
    params: Dict[str, str] = {}
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


def resolve_workfolder_pattern(
    path: Path, workfolder_pattern: str, co_commit_sha: str
) -> Path:
    assert isinstance(workfolder_pattern, str), f"Must be string {workfolder_pattern=}"
    variables = {
        "commitsha": co_commit_sha,
        "node": platform.node(),
    }
    workfolder = path.parent / workfolder_pattern.format(**variables)
    log.info("Workfolder {} resolved to {}".format(workfolder_pattern, workfolder))
    return workfolder


def run_experiment(path, co_commit, add_args):
    """
    Execute the Dervo experiment. Folder structure defines the experiment
    Args:
        - 'path' points to an experiment folder.
        - 'co_commit' if not RAW - check out and run that commit
        - 'add_args' are passed additionally to experiment
    """
    # Capture logs, before we establish location for logfiles
    with LogCaptorToRecords(pause="file") as lctr:
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
        # workfolder = mkdir(path.parent / FOLDER_OUTPUT / co_commit_sha)
        workfolder = resolve_workfolder_pattern(
            path, cfg["_dervo"]["workfolder"], co_commit_sha
        )

    # Setup logging in the workfolder
    id_string = get_experiment_id_string()
    logging_cfg = cfg["_dervo"]["logging"]
    logfilehandlers = add_logging_filehandlers(
        workfolder,
        id_string,
        logging_cfg.get("foldername"),
        logging_cfg.get("handlers", {}),
    )
    clamp_package_loglevels(logging_cfg.get("clamp_packages", {}))
    assert (run_string := cfg["_dervo"].get("run")), "_dervo.run must be defined"
    lctr.handle_captured()  # Release previously captured logging records
    dump_dervo_stats(workfolder, path, run_string, logfilehandlers)

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
