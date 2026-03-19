"""
Tools related to experiment organization
"""

import os.path
import inspect
import sys
import logging
from pathlib import Path

import yaml
from dervo.config import build_config_dag_inheritance, normpath

import vst

log = logging.getLogger(__name__)


def _resolve_config_path(path_: str, priority=["cfg.yml", "config.yml"]) -> Path:
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
    return normpath(path)  # Normalise, don't resolve symlinks


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
        path = _resolve_config_path(path)
        # Establish configuration
        ycfg = build_config_dag_inheritance(path)
        # Establish workfolder
        code_root = ycfg["_experiment"]["code_root"]
        assert code_root is not None, "code_root should be set"
        code_root = Path(code_root)
        # Establish commit to execute
        if co_commit is None:
            co_commit = ycfg["_experiment"]["commit"]
            log.info(
                "No commit passed, setting "
                "as _experiment.commit = {}".format(co_commit)
            )
        co_commit_sha, repo = get_commit_sha_repo(code_root, co_commit)
        workfolder = manage_workfolder(path, ycfg, co_commit_sha)
