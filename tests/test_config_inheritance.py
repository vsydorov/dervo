"""
Tests for config inheritance in dervo config.py

Covers:
- ^inherit: True (parent walk)
- ^inherit: [^parents] (explicit parent walk)
- ^inherit: [^parents, relative_path] (parents + mixin)
- ^inherit: [^parents, recipe] with recipe chaining
- Diamond inheritance (same ancestor via multiple paths, deduplicated)
- Sibling reference (inheriting from a sibling that itself inherits)
- No ^inherit (standalone config)
- Caret token resolution: ^root/..., ^relative, ^../ paths
- Merge order correctness (last writer wins)
"""

import os
from pathlib import Path

import pytest
import yaml
from omegaconf import OmegaConf as OC

from dervo.config import (
    build_config_dag_inheritance,
    build_dag,
    prune_dag,
    dag_dfs_to_merge_order,
    find_config_root,
    walk_parents,
    expand_inherit,
    resolve_caret_token,
    resolve_caret_tokens,
    abspath,
    dag_tree_str,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_yml(path: Path, data):
    """Write a dict or string as YAML."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        path.write_text(data)
    else:
        path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# Fixtures: build directory trees that mirror the real experiments
# ---------------------------------------------------------------------------


@pytest.fixture
def linear_tree(tmp_path):
    """
    Simple linear inheritance chain:
      root_cfg.yml  (sentinel, empty)
      group/cfg.yml  (^inherit: [^parents], sets code)
      group/exp/cfg.yml  (^inherit: True, sets run + overrides)
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {
            "^inherit": ["^parents"],
            "_dervo": {"code": "^root/../code/myproject"},
            "base_param": 1,
        },
    )
    write_yml(
        tmp_path / "group" / "exp" / "cfg.yml",
        {
            "^inherit": True,
            "_dervo": {"run": "src.train:main"},
            "base_param": 99,
            "exp_param": "hello",
        },
    )
    return tmp_path


@pytest.fixture
def mixin_tree(tmp_path):
    """
    Parents + mixin:
      root_cfg.yml
      group/cfg.yml  (^inherit: [^parents], base values)
      group/exp/cfg.yml  (^inherit: True, main experiment)
      group/exp/_mixin/cfg.yml  (no inherit, provides mixin: True)
      group/exp/sub/cfg.yml  (^inherit: [^parents, ^../_mixin/cfg.yml])
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {"^inherit": ["^parents"], "base_param": 1},
    )
    write_yml(
        tmp_path / "group" / "exp" / "cfg.yml",
        {"^inherit": True, "exp_param": 10},
    )
    write_yml(
        tmp_path / "group" / "exp" / "_mixin" / "cfg.yml",
        {"mixin": True},
    )
    write_yml(
        tmp_path / "group" / "exp" / "sub" / "cfg.yml",
        {"^inherit": ["^parents", "^../_mixin/cfg.yml"], "sub_param": 42},
    )
    return tmp_path


@pytest.fixture
def diamond_tree(tmp_path):
    """
    Diamond: base recipe reachable via two paths.
      root_cfg.yml
      group/cfg.yml  (^inherit: [^parents])
      group/scenario/cfg.yml  (^inherit: [^parents, _recipes/base.yml])
      group/scenario/_recipes/base.yml  (no inherit, sets model.lr)
      group/scenario/diamond/cfg.yml  (^inherit: [^parents, ../_recipes/base.yml])
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {"^inherit": ["^parents"], "base_param": 1},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "cfg.yml",
        {
            "^inherit": ["^parents", "_recipes/base.yml"],
            "scenario_param": "s",
        },
    )
    write_yml(
        tmp_path / "group" / "scenario" / "_recipes" / "base.yml",
        {"model": {"lr": 0.01, "weight_decay": 0.0001}},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "diamond" / "cfg.yml",
        {
            "^inherit": ["^parents", "../_recipes/base.yml"],
            "model": {"lr": 0.0005},
        },
    )
    return tmp_path


@pytest.fixture
def recipe_chain_tree(tmp_path):
    """
    Recipe-to-recipe inheritance:
      root_cfg.yml
      group/cfg.yml  (^inherit: [^parents])
      group/scenario/cfg.yml  (^inherit: [^parents, _recipes/base.yml])
      group/scenario/_recipes/base.yml  (no inherit)
      group/scenario/_recipes/advanced.yml  (^inherit: [base.yml])
      group/scenario/exp/cfg.yml  (^inherit: [^parents, ../_recipes/advanced.yml])
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {"^inherit": ["^parents"], "base_param": 1},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "cfg.yml",
        {
            "^inherit": ["^parents", "_recipes/base.yml"],
            "scenario_param": "s",
        },
    )
    write_yml(
        tmp_path / "group" / "scenario" / "_recipes" / "base.yml",
        {"model": {"lr": 0.01}, "trainer": {"max_epochs": 100}},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "_recipes" / "advanced.yml",
        {
            "^inherit": ["base.yml"],
            "model": {"lr": 0.001, "scheduler": "cosine"},
            "trainer": {"max_epochs": 200},
        },
    )
    write_yml(
        tmp_path / "group" / "scenario" / "exp" / "cfg.yml",
        {
            "^inherit": ["^parents", "../_recipes/advanced.yml"],
            "trainer": {"max_epochs": 300},
        },
    )
    return tmp_path


@pytest.fixture
def sibling_tree(tmp_path):
    """
    Sibling reference: one experiment inherits from another.
      root_cfg.yml
      group/cfg.yml
      group/scenario/cfg.yml
      group/scenario/_recipes/base.yml
      group/scenario/exp_a/cfg.yml  (^inherit: [^parents, ../_recipes/base.yml])
      group/scenario/exp_b/cfg.yml  (^inherit: [^parents, ../exp_a/cfg.yml])
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {"^inherit": ["^parents"], "base_param": 1},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "cfg.yml",
        {"^inherit": ["^parents"], "scenario_param": "s"},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "_recipes" / "base.yml",
        {"model": {"lr": 0.01}},
    )
    write_yml(
        tmp_path / "group" / "scenario" / "exp_a" / "cfg.yml",
        {
            "^inherit": ["^parents", "../_recipes/base.yml"],
            "seed": 42,
        },
    )
    write_yml(
        tmp_path / "group" / "scenario" / "exp_b" / "cfg.yml",
        {
            "^inherit": ["^parents", "../exp_a/cfg.yml"],
            "seed": 99,
        },
    )
    return tmp_path


@pytest.fixture
def standalone_tree(tmp_path):
    """
    No ^inherit at all — standalone config.
      root_cfg.yml
      group/cfg.yml  (has values, but standalone ignores it)
      group/standalone/cfg.yml  (no ^inherit)
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {"^inherit": ["^parents"], "should_not_appear": True},
    )
    write_yml(
        tmp_path / "group" / "standalone" / "cfg.yml",
        {"seed": 0, "model": {"lr": 0.0001}},
    )
    return tmp_path


@pytest.fixture
def caret_tokens_tree(tmp_path):
    """
    Various caret token patterns in config values.
      root_cfg.yml
      data/input.txt  (referenced file)
      group/cfg.yml  (uses ^root/ tokens)
      group/exp/cfg.yml  (uses ^../ and ^root/ tokens)
    """
    write_yml(tmp_path / "root_cfg.yml", "# sentinel\n")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "input.txt").write_text("dummy")
    write_yml(
        tmp_path / "group" / "cfg.yml",
        {
            "^inherit": ["^parents"],
            "paths": {"data": "^root/data", "code": "^root/../external_code"},
        },
    )
    write_yml(
        tmp_path / "group" / "exp" / "cfg.yml",
        {
            "^inherit": True,
            "paths": {"input": "^../input.txt"},
            "extra": "^root/data/input.txt",
        },
    )
    return tmp_path


# ===========================================================================
# Tests: find_config_root and walk_parents
# ===========================================================================


class TestConfigRoot:
    def test_find_root(self, linear_tree):
        cfg = linear_tree / "group" / "exp" / "cfg.yml"
        root = find_config_root(cfg.parent, "root_cfg.yml")
        assert root == linear_tree

    def test_find_root_missing(self, tmp_path):
        """No root sentinel → defaults to /."""
        (tmp_path / "a" / "b").mkdir(parents=True)
        root = find_config_root(tmp_path / "a" / "b", "root_cfg.yml")
        assert root == Path("/")

    def test_walk_parents(self, linear_tree):
        cfg = linear_tree / "group" / "exp" / "cfg.yml"
        root = find_config_root(cfg.parent, "root_cfg.yml")
        parents = walk_parents(cfg, root, "root_cfg.yml")
        # Should find group/cfg.yml, then root_cfg.yml
        assert len(parents) == 2
        assert parents[0] == linear_tree / "group" / "cfg.yml"
        assert parents[1] == linear_tree / "root_cfg.yml"


# ===========================================================================
# Tests: linear inheritance
# ===========================================================================


class TestLinearInheritance:
    def test_merge_order(self, linear_tree):
        cfg_path = linear_tree / "group" / "exp" / "cfg.yml"
        merged, caret_keys = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # exp overrides base_param
        assert c["base_param"] == 99
        assert c["exp_param"] == "hello"
        assert c["_dervo"]["run"] == "src.train:main"

    def test_code_caret_resolved(self, linear_tree):
        cfg_path = linear_tree / "group" / "exp" / "cfg.yml"
        merged, caret_keys = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        code_path = c["_dervo"]["code"]
        assert os.path.isabs(code_path)
        assert "myproject" in code_path


# ===========================================================================
# Tests: mixin inheritance
# ===========================================================================


class TestMixinInheritance:
    def test_mixin_merged(self, mixin_tree):
        cfg_path = mixin_tree / "group" / "exp" / "sub" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        assert c["mixin"] is True
        assert c["sub_param"] == 42
        assert c["exp_param"] == 10
        assert c["base_param"] == 1

    def test_mixin_does_not_override_sub(self, mixin_tree):
        """Sub's own values take priority over mixin."""
        cfg_path = mixin_tree / "group" / "exp" / "sub" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        assert c["sub_param"] == 42


"""
===========================================================================
Tests: diamond inheritance

├── group
│   ├── cfg.yml  (^inherit: ['^parents'])
│   └── scenario
│       ├── cfg.yml  (^inherit: ['^parents', '_recipes/base.yml'])
│       ├── diamond
│       │   └── cfg.yml  (^inherit: ['^parents', '../_recipes/base.yml'])
│       └── _recipes
│           └── base.yml
└── root_cfg.yml


Slop machine gets this one wrong and thinks that being reachable from two paths
implies deduplication. The base.yml should NOT be deduplicated, and should
appear twice. Consider the pruned DAG:
    
* ^root/group/scenario/diamond/cfg.yml  (^inherit: ['^parents', '../_recipes/base.yml'])
├── ^root/group/scenario/_recipes/base.yml
└── ^root/group/scenario/cfg.yml  (^inherit: ['^parents', '_recipes/base.yml'])
    ├── ^root/group/scenario/_recipes/base.yml
    └── ^root/group/cfg.yml  (^inherit: ['^parents'])
        └── ^root/root_cfg.yml

Config at "scenario/cfg.yml" does inherit _recipes/base.yml, but can also do
their own changes. Our experiment's  "diamond/cfg.yml" inherits
_recipes/base.yml after those changes, so base.yml should be applied twice

===========================================================================
"""


class TestDiamondInheritance:
    def test_base_appears_twice(self, diamond_tree):
        cfg_path = diamond_tree / "group" / "scenario" / "diamond" / "cfg.yml"
        root = find_config_root(cfg_path.parent, "root_cfg.yml")
        dag = build_dag(cfg_path, root, "root_cfg.yml")
        pruned = prune_dag(dag, cfg_path)
        order = dag_dfs_to_merge_order(pruned, cfg_path)
        base_path = diamond_tree / "group" / "scenario" / "_recipes" / "base.yml"
        assert order.count(base_path) == 2
        relpaths = [str(x.relative_to(root)) for x in order]
        assert relpaths == [
            "root_cfg.yml",
            "group/cfg.yml",
            "group/scenario/_recipes/base.yml",
            "group/scenario/cfg.yml",
            "group/scenario/_recipes/base.yml",
            "group/scenario/diamond/cfg.yml",
        ]

    def test_diamond_merge_values(self, diamond_tree):
        cfg_path = diamond_tree / "group" / "scenario" / "diamond" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # diamond overrides lr from base
        assert c["model"]["lr"] == 0.0005
        # weight_decay from base survives
        assert c["model"]["weight_decay"] == 0.0001
        assert c["scenario_param"] == "s"
        assert c["base_param"] == 1


"""
===========================================================================
Tests: recipe chain

This LLM also gets wrong with its deduplication. base.yml should appear twice.

.
├── group
│   ├── cfg.yml
│   └── scenario
│       ├── cfg.yml
│       ├── exp
│       │   └── cfg.yml   (^inherit: ['^parents', '../_recipes/advanced.yml'])
│       └── _recipes
│           ├── advanced.yml  (^inherit: ['base.yml'])
│           └── base.yml
└── root_cfg.yml

Again, consider the pruned DAG:

* ^root/group/scenario/exp/cfg.yml  (^inherit: ['^parents', '../_recipes/advanced.yml'])
├── ^root/group/scenario/_recipes/advanced.yml  (^inherit: ['base.yml'])
│   └── ^root/group/scenario/_recipes/base.yml
└── ^root/group/scenario/cfg.yml  (^inherit: ['^parents', '_recipes/base.yml'])
    ├── ^root/group/scenario/_recipes/base.yml
    └── ^root/group/cfg.yml  (^inherit: ['^parents'])
        └── ^root/root_cfg.yml

_recipes/advanced.yml inherits it, and scenario/cfg.yml, but then both apply
their own changes. Thus exp/cfg.yml will end up inheriting it twoce

===========================================================================
"""


class TestRecipeChain:
    def test_recipe_chain_order(self, recipe_chain_tree):
        cfg_path = recipe_chain_tree / "group" / "scenario" / "exp" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # exp overrides max_epochs (300 > advanced's 200 > base's 100)
        assert c["trainer"]["max_epochs"] == 300
        # scheduler from advanced survives
        assert c["model"]["scheduler"] == "cosine"
        # lr from advanced overrides base
        assert c["model"]["lr"] == 0.001

    def test_base_is_duplicated(self, recipe_chain_tree):
        cfg_path = recipe_chain_tree / "group" / "scenario" / "exp" / "cfg.yml"
        root = find_config_root(cfg_path.parent, "root_cfg.yml")
        dag = build_dag(cfg_path, root, "root_cfg.yml")
        pruned = prune_dag(dag, cfg_path)
        order = dag_dfs_to_merge_order(pruned, cfg_path)
        base = recipe_chain_tree / "group" / "scenario" / "_recipes" / "base.yml"
        assert order.count(base) == 2
        relpaths = [str(x.relative_to(root)) for x in order]
        assert relpaths == [
            "root_cfg.yml",
            "group/cfg.yml",
            "group/scenario/_recipes/base.yml",
            "group/scenario/cfg.yml",
            "group/scenario/_recipes/base.yml",
            "group/scenario/_recipes/advanced.yml",
            "group/scenario/exp/cfg.yml",
        ]


# ===========================================================================
# Tests: sibling reference
# ===========================================================================


class TestSiblingReference:
    def test_sibling_inherits_chain(self, sibling_tree):
        cfg_path = sibling_tree / "group" / "scenario" / "exp_b" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # exp_b overrides seed
        assert c["seed"] == 99
        # model.lr from base recipe (via exp_a's chain) survives
        assert c["model"]["lr"] == 0.01
        assert c["scenario_param"] == "s"
        assert c["base_param"] == 1


# ===========================================================================
# Tests: standalone (no ^inherit)
# ===========================================================================


class TestStandalone:
    def test_standalone_only_own_values(self, standalone_tree):
        cfg_path = standalone_tree / "group" / "standalone" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        assert c["seed"] == 0
        assert c["model"]["lr"] == 0.0001
        # Parent's value should NOT appear
        assert "should_not_appear" not in c


# ===========================================================================
# Tests: caret token resolution
# ===========================================================================


class TestCaretTokens:
    def test_root_token_resolved(self, caret_tokens_tree):
        cfg_path = caret_tokens_tree / "group" / "exp" / "cfg.yml"
        merged, caret_keys = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # ^root/data should resolve to <root>/data
        assert os.path.isabs(c["paths"]["data"])
        assert c["paths"]["data"].endswith("/data")

    def test_relative_caret_resolved(self, caret_tokens_tree):
        cfg_path = caret_tokens_tree / "group" / "exp" / "cfg.yml"
        merged, caret_keys = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        # ^../input.txt relative to group/exp/ → group/input.txt
        assert os.path.isabs(c["paths"]["input"])

    def test_caret_keys_tracked(self, caret_tokens_tree):
        cfg_path = caret_tokens_tree / "group" / "exp" / "cfg.yml"
        merged, caret_keys = build_config_dag_inheritance(cfg_path)
        # caret_keys should track all resolved ^ tokens
        assert len(caret_keys) > 0
        for key, resolved in caret_keys.items():
            assert os.path.isabs(resolved), f"{key} should be absolute: {resolved}"

    def test_root_parent_path_resolved(self, caret_tokens_tree):
        """^root/../external_code should resolve to path above root."""
        cfg_path = caret_tokens_tree / "group" / "exp" / "cfg.yml"
        merged, _ = build_config_dag_inheritance(cfg_path)
        c = OC.to_container(merged, resolve=True)
        assert os.path.isabs(c["paths"]["code"])
        assert "external_code" in c["paths"]["code"]


# ===========================================================================
# Tests: glob caret tokens  (resolve_caret_token directly)
# ===========================================================================


@pytest.fixture
def glob_tree(tmp_path):
    """
    Simulate experiment output folders with different mtimes:
      group/producer/OUT/aaa/result.txt   (mtime=1000)
      group/producer/OUT/bbb/result.txt   (mtime=2000)
      group/producer/OUT/ccc/result.txt   (mtime=1500)
    """
    out = tmp_path / "group" / "producer" / "OUT"
    for name, ts in [("aaa", 1000), ("bbb", 2000), ("ccc", 1500)]:
        f = out / name / "result.txt"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(name)
        os.utime(f, (ts, ts))
        os.utime(f.parent, (ts, ts))
    return tmp_path


class TestGlobCaretTokens:
    """Tests for ^path[^sort][^sel] glob resolution."""

    def _resolve(self, glob_tree, token):
        root = glob_tree
        cwd = glob_tree / "group" / "consumer"
        return resolve_caret_token(token, root, cwd)

    def test_default_returns_oldest(self, glob_tree):
        """No sort/sel → mtime_asc^0 → oldest."""
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt")
        assert r.endswith("/aaa/result.txt")

    def test_newest(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^newest")
        assert r.endswith("/bbb/result.txt")

    def test_oldest_explicit(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^oldest")
        assert r.endswith("/aaa/result.txt")

    def test_mtime_desc_is_newest(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^mtime_desc")
        assert r.endswith("/bbb/result.txt")

    def test_name_asc(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^name_asc")
        assert r.endswith("/aaa/result.txt")

    def test_name_desc(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^name_desc")
        assert r.endswith("/ccc/result.txt")

    def test_list_newest(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^newest^list")
        assert isinstance(r, list)
        assert len(r) == 3
        assert r[0].endswith("/bbb/result.txt")
        assert r[-1].endswith("/aaa/result.txt")

    def test_list_name_asc(self, glob_tree):
        r = self._resolve(glob_tree, "^../producer/OUT/*/result.txt^name_asc^list")
        assert isinstance(r, list)
        assert r[0].endswith("/aaa/result.txt")
        assert r[-1].endswith("/ccc/result.txt")

    def test_no_matches_raises(self, glob_tree):
        with pytest.raises((FileNotFoundError, AssertionError)):
            self._resolve(glob_tree, "^../producer/OUT/*/nonexistent.xyz")

    def test_unknown_select_raises(self, glob_tree):
        with pytest.raises(RuntimeError):
            self._resolve(glob_tree, "^../producer/OUT/*/result.txt^newest^bogus")

    def test_no_glob_passthrough(self, glob_tree):
        """Non-glob token returns normalized path without globbing."""
        r = self._resolve(glob_tree, "^../producer/OUT/aaa/result.txt")
        assert os.path.isabs(r)
        assert r.endswith("/producer/OUT/aaa/result.txt")

    def test_inherit_forbids_list(self, glob_tree):
        """^inherit with a glob that resolves to list → ValueError."""
        root = glob_tree
        cwd = glob_tree / "group" / "consumer"
        with pytest.raises(ValueError, match="Forbidden"):
            expand_inherit(
                ["^../producer/OUT/*/result.txt^oldest^list"],
                cwd / "cfg.yml",
                root,
                "root_cfg.yml",
            )
