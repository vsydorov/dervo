# Checking out
import shutil
import time
import logging
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import (
        Dict, NamedTuple, List, TypeVar, Union, Tuple, Any, Callable, Optional)

import git

import vst

log = logging.getLogger(__name__)


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
            log.debug('Dirty repo diff:\n===\n{}\n==='.format(dirty_diff))
    except git.exc.InvalidGitRepositoryError:
        log.info('No git repo found')
        repo, commit_sha, dirty = None, None, False

    return repo, commit_sha, dirty


def git_get_hexsha(repo, co_commit):
    # Assign hexsha for commit we are trying to exract
    try:
        git_commit = repo.commit(co_commit)
        co_commit_sha = git_commit.hexsha
        log.info('Commit sha is {}'.format(co_commit_sha))
    except git.BadName as e:
        log.warning('Improper commit_sha {}'.format(co_commit))
        raise e
    return co_commit_sha


def git_shared_clone(repo, rpath, co_repo_fold, commit_sha):
    # Checkout proper commit
    repo.git.clone('--shared', rpath, str(co_repo_fold))
    co_repo = git.Repo(str(co_repo_fold))
    co_repo.git.checkout(commit_sha)
    co_repo.close()


def git_repo_perform_checkout_and_postcmd(
        repo, co_repo_fold, co_commit_sha, post_cmd, n_post_cmd_tries=2):
    """
    Checkout repo to co_repo_fold, copy submodules, run post_cmd code
    """
    # Create nice repo folder
    vst.mkdir(co_repo_fold)
    git_shared_clone(repo, '.', co_repo_fold, co_commit_sha)
    # Submodules cloned individually (avoid querying the remote)
    submodules = repo.git.submodule('status').split('\n')
    submodules = [x.strip().split(' ')[:2] for x in submodules]
    for commit_sha, subfold in submodules:
        git_shared_clone(repo, subfold, co_repo_fold/subfold, commit_sha)
    # Perform post-checkout actions if set
    if post_cmd is not None:
        post_output = None
        for i in range(n_post_cmd_tries):
            try:
                post_output = subprocess.check_output(
                        f'cd {co_repo_fold} && {post_cmd}',
                        shell=True, stderr=subprocess.STDOUT,
                        executable='/bin/bash').strip().decode()
                break
            except subprocess.CalledProcessError as e:
                log.info('({}) Waiting a bit. Caught ({}):\n{}'.format(
                    i, e, e.output.decode()))
                time.sleep(5)
        if post_output is None:
            raise OSError(f'Could not execute {post_cmd}')
        log.info(f'Executed {post_cmd} at {co_repo_fold}')
        log.debug(f'Output of execution:\n{post_output}')
    # Create 'FINISHED' file to indicate that repo is ready
    (co_repo_fold/'FINISHED').touch()


def git_repo_is_checkout_complete(co_repo_fold: Path, co_commit_sha: str):
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


def git_repo_careful_checkout(
        repo, co_repo_fold, co_commit_sha, post_cmd, n_repo_checks=2):
    """
    Checkout repo carefully.
    - If folder already exists - wait a bit and check if the repo is good
    - If repo is bad - create alternative folder for checkout
    """
    if not co_repo_fold.exists():
        git_repo_perform_checkout_and_postcmd(
                repo, co_repo_fold, co_commit_sha, post_cmd)
        log.info(f'Checked out code to {co_repo_fold}')
    else:
        # Wait a bit (maybe repo is being checked out by another job)
        for i in range(n_repo_checks):
            co_good = git_repo_is_checkout_complete(co_repo_fold, co_commit_sha)
            if co_good:
                log.info(f'Found good, checked out repo at {co_repo_fold}')
                break
            log.info(f'({i}) Waiting for checked out folder '
                    f'to appear at {co_repo_fold}')
            time.sleep(5)
        # If waiting did not help - create alternative folder
        if not co_good:
            datetime_now = datetime.now().strftime('%Y-%m-%d_%H-%m_')
            co_repo_fold = Path(tempfile.mkdtemp(prefix=datetime_now,
                    dir=str(co_repo_fold.parent), suffix='temp'))
            git_repo_perform_checkout_and_postcmd(
                    repo, co_repo_fold, co_commit_sha, post_cmd)
            log.info(f'Checked out code to alternative '
                    f'location {co_repo_fold}')
    return co_repo_fold


"""
Managing code (wrt git commits), obtaining well formed prefix
- When co_commit is None:
    - Run from "code_root", no checkout, set prefix to 'RAW'
- When co_commit is set:
    - Checkout
    - Make sure repo exists and is in good condition.
    - Try avoiding concurrency problems.
    - Prefix is SHA
"""


def get_commit_sha_repo(code_root, co_commit):
    # Query repo here, even if RAW
    repo, commit_sha, dirty = git_repo_query(code_root)
    # Maybe no checkout is needed?
    if co_commit == 'RAW':
        log.info(f'Running raw code at {code_root}')
        return 'RAW', None
    assert (repo is not None) and (commit_sha is not None), 'repo must exist'
    if co_commit == 'HEAD':
        # co_commit is either SHA or HEAD. If 'HEAD', must not be dirty
        assert not dirty, ('We disallow checking out HEAD of dirty repo. '
                'Call with "--raw" or provide commit sha')
    co_commit_sha = git_get_hexsha(repo, co_commit)
    return co_commit_sha, repo


def manage_code_checkout(
        repo, co_commit_sha: str, workfolder: Path,
        code_root: Path, checkout_root: str,
        to_workfolder: bool, post_cmd: str,
        ) -> Path:
    log.info(f'Checking out code from {code_root}')

    if to_workfolder:
        # This will ensure a copy of the code is in the workfolder
        destination = workfolder/'_code'
        if co_commit_sha != 'RAW':
            # Checkout commit directly
            destination = git_repo_careful_checkout(
                    repo, destination, co_commit_sha, post_cmd)
        else:
            if destination.exists():
                shutil.rmtree(destination, ignore_errors=True)
            shutil.copytree(code_root, destination, dirs_exist_ok=True)
    else:
        if co_commit_sha != 'RAW':
            co_repo_basename = code_root.name
            assert checkout_root is not None, 'checkout_root should be set'
            destination = Path(checkout_root)/f'{co_repo_basename}/{co_commit_sha}'
            destination = git_repo_careful_checkout(
                    repo, destination, co_commit_sha, post_cmd)
        else:
            destination = code_root
    return destination
