import copy
import logging
import os.path
import re
from glob import glob
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

import yaml
from omegaconf import DictConfig
from omegaconf import OmegaConf as OC
from omegaconf import open_dict

from dervo.misc import abspath

log = logging.getLogger(__name__)

# Pretty formatter for DictConfig
setattr(
    DictConfig, "_repr_pretty_", lambda self, p, cycle: p.pretty(OC.to_container(self))
)

# Hardcoded filenames
ROOT_PREFIX = "root_"  # Snake stops after seeing this ROOT prefix
DERVO_DEFAULTS = Path(__file__).parent / "defaults.yml"


GLOB_CHARS = re.compile(r"[*?\[]")


def resolve_caret_token(token: str, root: Path, cwd: Path) -> Union[str, List[str]]:
    """
    Caret token is a string like ^<ppath>[^<sort>][^<select>] and resolves to
    string, or (rarely) a list of strings, if <select> component is "list"

    Supported <ppath> values. Can contain glob components.
      - ^root/<path>         : (special value) relative to dervo root
      - ^/<path>             : absolute filesystem path
      - ^./<path>            : relative to current config folder
      - ^../<path>           : relative to parent folder

    Supported <sort> values
      - mtime_asc, oldest    : Sort matches by ascending mtime, from oldest. (Default)
      - mtime_desc, newest   : Sort matches by descending mtime.
      - name_asc             : Sort matches lexically, by ascending name
      - name_desc            : Sort matches lexically, by descending name.

    Supported <select> values
      - only                 : Take first match, ensure only one exists. (Default)
      - 0, first             : Take first match
      - list                 : Return all matches as a list
    """
    if not isinstance(token, str) or not token.startswith("^"):
        raise RuntimeError(f"Can't caret resolve {token=}")

    # Parse caret components here with regexp
    m = re.fullmatch(r"\^([^^]+?)(?:\^([^^]+))?(?:\^([^^]+))?", token)
    assert m is not None, f"{token=} can't be parsed"
    comp_ppath, comp_sort, comp_select = m.group(1), m.group(2), m.group(3)

    # Parse pprefix
    if comp_ppath.startswith("root/"):
        ppath = os.path.join(root, comp_ppath.removeprefix("root/"))
    elif comp_ppath.startswith("/"):
        ppath = comp_ppath  # Already absolute
    elif comp_ppath.startswith(("./", "../")):
        ppath = os.path.join(cwd, comp_ppath)
    else:
        ppath = os.path.join(cwd, comp_ppath)
        log.warning(f"Underdefined {token=}. Treating as './', see {ppath=}")

    # If not glob -> straighforward return
    if not GLOB_CHARS.search(ppath):
        return os.path.normpath(ppath)

    # * Assuming globs found -> run glob, select
    if comp_sort is None:
        comp_sort = "mtime_asc"
    if comp_select is None:
        comp_select = "only"
    # ** Sort
    # Older python version dont have include_hidden
    try:
        matches = glob(os.path.normpath(ppath), recursive=True, include_hidden=True)
    except TypeError:
        matches = glob(os.path.normpath(ppath), recursive=True)

    if comp_sort == "name_asc":
        matches = sorted(matches)
    elif comp_sort == "name_desc":
        matches = sorted(matches, reverse=True)
    else:
        stats = {m: os.stat(m) for m in matches}
        if comp_sort in ["mtime_asc", "oldest"]:
            matches = sorted(matches, key=lambda m: stats[m].st_mtime)
        elif comp_sort in ["mtime_desc", "newest"]:
            matches = sorted(matches, key=lambda m: stats[m].st_mtime, reverse=True)
        else:
            raise RuntimeError(f"Unknown {comp_sort=} in {token=}")

    # ** Select
    if comp_select in ["only", "0", "first"]:
        assert len(matches), f"Must have matches for {ppath=} to {comp_select=}"
        if comp_select == "only":
            assert len(matches) == 1, f"Too many ({len(matches)} matches for {ppath=}"
        return matches[0]
    elif comp_select == "list":
        return matches
    else:
        raise RuntimeError(f"Unkown {comp_select=} in {token=}")


class CaretAnnotation(str):
    """str subclass carrying the original caret token, for annotated YAML display."""

    token: str

    def __new__(cls, value, token):
        obj = str.__new__(cls, value)
        obj.token = token
        return obj


class _CaretDumper(yaml.Dumper):
    pass


_CaretDumper.add_representer(
    CaretAnnotation,
    lambda dumper, data: dumper.represent_scalar(
        "tag:yaml.org,2002:str", f"{str(data)}  (<- {data.token})"
    ),
)


def _walk_omegaconf_leaves(obj, prefix="") -> Iterator[Tuple[str, Any]]:
    """Yield (key_path, value) for all leaf nodes in a nested dict/list structure."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_omegaconf_leaves(v, f"{prefix}.{k}" if prefix else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk_omegaconf_leaves(v, f"{prefix}[{i}]")
    else:
        yield prefix, obj


def resolve_caret_tokens(
    cfg: DictConfig, cfg_path: Path, root: Path
) -> Tuple[DictConfig, Dict[str, tuple[str, Union[str, List[str]]]]]:
    """Resolve ^-prefixed string values in cfg, working on a copy.
    Returns (resolved_cfg, {key: (token, resolved_path)})."""
    cfg = copy.deepcopy(cfg)
    caret_resolutions: Dict[str, tuple[str, Union[str, List[str]]]] = {}
    for key, val in _walk_omegaconf_leaves(OC.to_container(cfg, resolve=False)):
        if isinstance(val, str) and val.startswith("^"):
            resolved = resolve_caret_token(val, root, cfg_path.parent)
            caret_resolutions[key] = (val, resolved)
            OC.update(cfg, key, resolved)
    return cfg, caret_resolutions


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


def expand_inherit(
    inherit: Union[bool, List[str]], cfg_path: Path, root: Path, stopfilename: str
) -> List[Path]:
    """Expand ^inherit clause into list of source paths, bottom-up order."""
    if inherit is True:
        return walk_parents(cfg_path, root, stopfilename)
    if inherit is False:
        return []
    # Process items in reverse order (last item = highest priority)
    # Each item's internal expansion keeps its own order
    sources: List[Path] = []
    for item in reversed(inherit):
        if item == "^parents":
            sources.extend(walk_parents(cfg_path, root, stopfilename))
        else:
            # Force resolve as relative to current cfg_path if not caret token.
            if not item.startswith("^"):
                item = "^./" + item
            resolved = resolve_caret_token(item, root, cfg_path.parent)
            if isinstance(resolved, list):
                raise ValueError(
                    f"Forbidden expansion {item=} {resolved=} inside ^inherit"
                )
            sources.append(Path(resolved))
    return sources


TYPE_DAG = Dict[Path, Dict]


def build_dag(start: Path, root: Path, stopfilename: str) -> TYPE_DAG:
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
        assert isinstance(raw, DictConfig), f"{cfg_path=} {raw=} not DictConfig"
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


def prune_dag(dag: dict, start: Optional[Path] = None) -> TYPE_DAG:
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
    closures: Dict[Path, Set[Path]] = {}
    for node in topo:
        closure = {node}
        for src in dag[node]["sources"]:
            closure |= closures[src]
        closures[node] = closure

    # Prune: for each node, walk sources, skip those fully covered
    pruned: TYPE_DAG = {}
    for node, info in dag.items():
        covered: Set[Path] = set()
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


def dag_tree_str(
    dag: dict, start: Optional[Path] = None, root: Optional[Path] = None
) -> str:
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
    if "^inherit" in cfg:
        cfg = copy.deepcopy(cfg)
        with open_dict(cfg):
            del cfg["^inherit"]
    return cfg


def _configs_to_merge_str(
    cfgs_resolved: Dict[Path, DictConfig],
    caret_resolutions: Dict[Path, Dict[str, tuple[str, Union[str, List[str]]]]],
    root,
) -> str:
    """Format resolved configs for logging, annotating caret-resolved values inline."""
    lines = ["Configs to merge:", "======="]
    for lvl, (p, cfg) in enumerate(cfgs_resolved.items()):
        lines.append(f"--- {lvl}: {_rel_to_root(p, root)} ---")
        container = OC.to_container(cfg, resolve=False)
        for key, (token, _) in caret_resolutions[p].items():
            parts = key.split(".")
            obj: Any = container
            for part in parts[:-1]:
                obj = obj[part]
            value = obj[parts[-1]]
            if isinstance(value, list):
                value = str(value)
            obj[parts[-1]] = CaretAnnotation(value, token)
        lines.append(
            yaml.dump(
                container, Dumper=_CaretDumper, default_flow_style=False, width=256
            ).rstrip()
        )
        lines.append("")
    if lines[-1] == "":
        lines.pop()
    lines.append("=======")
    return "\n".join(lines)


def build_config_dag_inheritance(
    start: Path,
) -> Tuple[DictConfig, Dict[str, Union[str, List[str]]]]:
    """Load cfg file, resolve ^inherit DAG, merge all in order."""
    start = abspath(start)
    stopfilename = ROOT_PREFIX + start.name
    root = find_config_root(start.parent, stopfilename)

    dag = build_dag(start, root, stopfilename)
    log.info("Config tree (full):\n" + dag_tree_str(dag, start, root))

    pruned_dag = prune_dag(dag, start)
    log.info("Config tree (pruned):\n" + dag_tree_str(pruned_dag, start, root))

    # * Before merging
    # ** Order by dfs
    cfg_paths_ordered = dag_dfs_to_merge_order(pruned_dag, start)
    # ** Put dervo defaults at lowest priority
    if DERVO_DEFAULTS.exists():
        cfg_paths_ordered = [DERVO_DEFAULTS] + cfg_paths_ordered
    # ** Remove _inherit keys (their purpose has been served)
    cfgs_ordered_ = {}
    for cfg_path in cfg_paths_ordered:
        loaded = OC.load(cfg_path)
        assert isinstance(loaded, DictConfig), f"{cfg_path=} {loaded=} not DictConfig"
        cfgs_ordered_[cfg_path] = _strip_inherit(loaded)
    # ** Remove empty configs
    cfgs_ordered = {p: cfg for p, cfg in cfgs_ordered_.items() if len(cfg)}
    # ** Print merge order
    lines = ["Merge order:"]
    for lvl, (p, cfg) in enumerate(cfgs_ordered_.items()):
        lines.append(f"  {lvl}: {_rel_to_root(p, root)}")
        if len(cfg) == 0:
            lines[-1] += " [EMPTY]"
    log.info("\n".join(lines) + "\n")
    # ** Resolve caret tokens per-config, record
    cfgs_resolved: Dict[Path, DictConfig] = {}
    caret_resolutions: Dict[Path, Dict[str, tuple[str, Union[str, List[str]]]]] = {}
    for p, cfg in cfgs_ordered.items():
        cfgs_resolved[p], caret_resolutions[p] = resolve_caret_tokens(cfg, p, root)
    # ** Print configs and caret resolutions
    log.info(_configs_to_merge_str(cfgs_resolved, caret_resolutions, root))

    # Merge. Build caret_keys: flat {key: resolved} for all keys that originated
    # as caret tokens — last-writer-wins, matching merge priority order.
    if len(cfgs_resolved) == 0:
        log.warning("Empty final config")
        return OC.create(), {}
    merged = OC.merge(*cfgs_resolved.values())
    assert isinstance(merged, DictConfig), f"{merged=} not DictConfig"
    caret_keys: Dict[str, Union[str, List[str]]] = {}
    for p in cfgs_resolved:
        caret_keys.update(
            {k: resolved for k, (token, resolved) in caret_resolutions[p].items()}
        )

    lines = ["Merged config (dervo):", "======"]
    lines.append(OC.to_yaml(merged, resolve=False).rstrip())
    lines.append("======")
    log.info("\n".join(lines))
    return merged, caret_keys
