import os.path
import logging
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Dict, List

import yaml
from omegaconf import DictConfig, OmegaConf as OC

import vst

log = logging.getLogger(__name__)

# Pretty formatter for DictConfig
DictConfig._repr_pretty_ = lambda self, p, cycle: p.pretty(OC.to_container(self))

# Hardcoded filenames
ROOT_PREFIX = "root_"  # Snake stops after seeing this ROOT prefix


def normpath(path):
    return Path(os.path.normpath(path))


def find_config_root(start: Path, stopfilename: str) -> Path:
    for p in [start, *start.parents]:
        if (p / stopfilename).exists():
            return p
    log.warning(
        f"{stopfilename} not found above {start}, defaulting to filesystem root /"
    )
    return Path("/")


def walk_parents(cfg_path: Path, root: Path, stopfilename: str) -> list:
    """Snake walk: collect same-named cfg files going up from parent dir to root.
    Returns bottom-up order (nearest parent first, root sentinel last)."""
    name = cfg_path.name
    found = []
    for folder in cfg_path.parent.parents:
        candidate = folder / name
        if candidate.exists():
            found.append(candidate)
        if folder == root:
            break
    # Append root sentinel at the end (deepest ancestor = last)
    sentinel = root / stopfilename
    if sentinel.exists() and sentinel not in found:
        found.append(sentinel)
    return found


def expand_inherit(inherit, cfg_path: Path, root: Path, stopfilename: str) -> list:
    """Expand ^inherit clause into list of source paths, bottom-up order."""
    if inherit is True:
        return walk_parents(cfg_path, root, stopfilename)
    # Process items in reverse order (last item = highest priority = first in bottom-up)
    # but each item's internal expansion keeps its own order
    sources = []
    for item in reversed(inherit):
        if item == "^parents":
            sources.extend(walk_parents(cfg_path, root, stopfilename))
        elif str(item).startswith("^root/"):  # TODO: Unify this
            sources.append(root / str(item)[len("^root/") :])
        else:
            sources.append((cfg_path.parent / item).resolve())
    return sources


def build_dag(start: Path, root: Path, stopfilename: str) -> dict:
    """Build inheritance DAG bottom-up from start node.
    Returns {path: {'inherit': raw_clause, 'sources': [deps in bottom-up order]}}.
    """
    dag = {}
    queue = [start]
    while queue:
        cfg_path = queue.pop(0)
        if cfg_path in dag:
            continue
        raw = OC.load(cfg_path)
        inherit = raw.get("^inherit", None)
        sources = (
            expand_inherit(inherit, cfg_path, root, stopfilename)
            if inherit is not None
            else []
        )
        dag[cfg_path] = {"inherit": inherit, "sources": sources}
        for src in sources:
            if src not in dag:
                queue.append(src)
    return dag


def prune_dag(dag: dict, start: Path = None) -> dict:
    """Prune redundant sources from DAG. A source is redundant if its entire
    transitive closure is already covered by previously-processed sources.
    Returns a new DAG with pruned sources and only reachable nodes."""
    if start is None:
        start = next(iter(dag))

    # Compute transitive closures bottom-up (leaves first)
    topo = list(
        TopologicalSorter(
            {n: info["sources"] for n, info in dag.items()}
        ).static_order()
    )
    closures = {}
    for node in topo:
        closure = {node}
        for src in dag[node]["sources"]:
            closure |= closures[src]
        closures[node] = closure

    # Prune: for each node, walk sources, skip those fully covered
    pruned = {}
    for node, info in dag.items():
        covered = set()
        new_sources = []
        for src in info["sources"]:
            if closures[src] <= covered:
                continue
            new_sources.append(src)
            covered |= closures[src]
        pruned[node] = {"inherit": info["inherit"], "sources": new_sources}

    return pruned


def dag_dfs_to_merge_order(dag: dict, start: Path) -> list:
    """DFS post-order: deepest ancestors first, start node last.
    Sources are reversed so lowest-priority (root) is processed first."""
    ordered = []

    def dfs(node):
        for src in reversed(dag[node]["sources"]):
            dfs(src)
        ordered.append(node)

    dfs(start)
    return ordered


def _rel_to_root(p: Path, root: Path) -> str:
    """Show path relative to root with ^root/ prefix, or absolute if outside root."""
    try:
        return "^root/" + str(p.relative_to(root))
    except ValueError:
        return str(p)


def dag_tree_str(dag: dict, start: Path = None, root: Path = None) -> str:
    """Plain text tree of the DAG, showing full expansion without deduplication."""
    if start is None:
        start = next(iter(dag))
    if root is None:
        root = next(p for p, info in dag.items() if not info["sources"]).parent

    lines = []

    def rel(p):
        return _rel_to_root(p, root)

    def inherit_str(p):
        inherit = dag[p]["inherit"]
        if inherit is None:
            return ""
        return f"  (^inherit: {inherit})"

    def walk(p, prefix=""):
        sources = dag[p]["sources"]
        for i, dep in enumerate(sources):
            last = i == len(sources) - 1
            connector = "└── " if last else "├── "
            lines.append(f"{prefix}{connector}{rel(dep)}{inherit_str(dep)}")
            walk(dep, prefix + ("    " if last else "│   "))

    lines.append(f"* {rel(start)}{inherit_str(start)}")
    walk(start)
    return "\n".join(lines)


def _strip_inherit(cfg: DictConfig) -> DictConfig:
    keys = [k for k in cfg if str(k) != "^inherit"]
    return OC.masked_copy(cfg, keys)


def build_config_dag_inheritance(start: Path) -> DictConfig:
    """Load cfg file, resolve ^inherit DAG, merge all in order."""
    start = normpath(start)
    stopfilename = ROOT_PREFIX + start.name
    root = find_config_root(start.parent, stopfilename)

    dag = build_dag(start, root, stopfilename)
    log.info("Config tree (full):\n" + dag_tree_str(dag, start, root))

    pruned_dag = prune_dag(dag, start)
    log.info("Config tree (pruned):\n" + dag_tree_str(pruned_dag, start, root))

    # Before merging: Remote empty, print names
    ordered_cfg_paths = dag_dfs_to_merge_order(pruned_dag, start)
    ordered_cfgs = {}
    for cfg_path in ordered_cfg_paths:
        ordered_cfgs[cfg_path] = _strip_inherit(OC.load(cfg_path))
    ordered_cfgs_nonempty = {p: cfg for p, cfg in ordered_cfgs.items() if len(cfg)}
    lines = ["Merge order:"]
    for lvl, (p, cfg) in enumerate(ordered_cfgs.items()):
        lines.append(f"  {lvl}: {_rel_to_root(p, root)}")
        if len(cfg) == 0:
            lines[-1] += " [EMPTY]"
    log.info("\n".join(lines) + "\n")

    # Before merging: Print contents of non-empty
    lines = ["Configs to merge:", "======="]
    for lvl, (p, cfg) in enumerate(ordered_cfgs_nonempty.items()):
        lines.append(f"--- {lvl}: {_rel_to_root(p, root)} ---")
        lines.append(OC.to_yaml(cfg, resolve=False).rstrip())
        lines.append("")
    if lines[-1] == "":
        lines.pop()
    lines.append("=======")
    log.info("\n".join(lines) + "\n")

    # OC merge, print
    if len(ordered_cfgs_nonempty) == 0:
        log.warning("Empty final config")
        return OC.create()
    merged = OC.merge(*ordered_cfgs_nonempty.values())
    lines = ["Merged config (dervo):", "======"]
    lines.append(OC.to_yaml(merged, resolve=False).rstrip())
    lines.append("======")
    log.info("\n".join(lines))

    return merged
