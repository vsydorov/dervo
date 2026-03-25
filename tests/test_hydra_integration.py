"""
Tests for hydra integration in dervo experiment.py

Three scenarios:
1. @hydra.main decorated function, no _hydra in dervo config
   → detect from closure, run as hydra experiment
2. Plain function, _hydra specified in dervo config
   → use config-specified params, run as hydra experiment
3. Plain function, no _hydra in config
   → plain call, no hydra
"""

import os
import sys
import inspect
import importlib
from pathlib import Path

import pytest
import yaml
from omegaconf import DictConfig, OmegaConf as OC

from dervo.experiment import (
    get_hydra_closure_params,
    _query_update_hydra_params,
    _hydra_update_config,
)


# ---------------------------------------------------------------------------
# Fixtures: real code files and hydra config dirs
# ---------------------------------------------------------------------------


@pytest.fixture
def hydra_config_dir(tmp_path):
    """Minimal hydra config directory with train.yaml."""
    conf_dir = tmp_path / "configs"
    conf_dir.mkdir()
    (conf_dir / "train.yaml").write_text(
        yaml.dump(
            {"model": {"name": "default_model", "layers": 3}, "lr": 0.001},
            default_flow_style=False,
        )
    )
    return conf_dir


@pytest.fixture
def hydra_config_dir_with_groups(tmp_path):
    """Config dir with a config group (model=big)."""
    conf_dir = tmp_path / "configs"
    conf_dir.mkdir()
    (conf_dir / "train.yaml").write_text(
        yaml.dump(
            {"defaults": [{"model": "small"}, "_self_"], "lr": 0.001},
            default_flow_style=False,
        )
    )
    model_dir = conf_dir / "model"
    model_dir.mkdir()
    (model_dir / "small.yaml").write_text(
        yaml.dump({"name": "small_model", "layers": 2}, default_flow_style=False)
    )
    (model_dir / "big.yaml").write_text(
        yaml.dump({"name": "big_model", "layers": 12}, default_flow_style=False)
    )
    return conf_dir


@pytest.fixture
def hydra_module(tmp_path, hydra_config_dir):
    """Create a real Python module with @hydra.main decorated function."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "__init__.py").write_text("")
    (src_dir / "train_hydra.py").write_text(
        f"""\
import hydra
from omegaconf import DictConfig

@hydra.main(version_base="1.3", config_path="{hydra_config_dir}", config_name="train.yaml")
def main(cfg: DictConfig):
    return cfg
"""
    )
    sys.path.insert(0, str(tmp_path))
    mod = importlib.import_module("src.train_hydra")
    yield mod
    sys.path.remove(str(tmp_path))
    for k in list(sys.modules):
        if k.startswith("src"):
            del sys.modules[k]


@pytest.fixture
def hydra_module_relative(tmp_path):
    """Module with @hydra.main using relative config_path (like real projects)."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "__init__.py").write_text("")
    conf_dir = src_dir / "configs"
    conf_dir.mkdir()
    (conf_dir / "train.yaml").write_text(
        yaml.dump(
            {"model": {"name": "default_model", "layers": 3}, "lr": 0.001},
            default_flow_style=False,
        )
    )
    (src_dir / "train_hydra_rel.py").write_text(
        """\
import hydra
from omegaconf import DictConfig

@hydra.main(version_base="1.3", config_path="configs", config_name="train.yaml")
def main(cfg: DictConfig):
    return cfg
"""
    )
    sys.path.insert(0, str(tmp_path))
    mod = importlib.import_module("src.train_hydra_rel")
    yield mod
    sys.path.remove(str(tmp_path))
    for k in list(sys.modules):
        if k.startswith("src"):
            del sys.modules[k]


@pytest.fixture
def plain_module(tmp_path):
    """Create a real Python module with a plain (non-hydra) function."""
    src_dir = tmp_path / "src"
    src_dir.mkdir(exist_ok=True)
    (src_dir / "__init__.py").write_text("")
    (src_dir / "train_plain.py").write_text(
        """\
from omegaconf import DictConfig

def main(cfg: DictConfig):
    return cfg

def main_with_args(cfg: DictConfig, add_args=None):
    return cfg, add_args
"""
    )
    sys.path.insert(0, str(tmp_path))
    mod = importlib.import_module("src.train_plain")
    yield mod
    sys.path.remove(str(tmp_path))
    for k in list(sys.modules):
        if k.startswith("src"):
            del sys.modules[k]


@pytest.fixture(autouse=True)
def clean_global_hydra():
    """Ensure GlobalHydra is clean before and after each test."""
    from hydra.core.global_hydra import GlobalHydra

    GlobalHydra.instance().clear()
    yield
    GlobalHydra.instance().clear()


# ===========================================================================
# Scenario 1: @hydra.main decorated, no _hydra in dervo config
# ===========================================================================


class TestHydraFromClosure:
    def test_closure_params_extracted(self, hydra_module):
        """get_hydra_closure_params extracts params from real @hydra.main."""
        params = get_hydra_closure_params(hydra_module.main)
        assert params.get("config_name") == "train.yaml"
        assert os.path.isabs(params["config_path"])

    def test_closure_params_relative_resolved(self, hydra_module_relative):
        """Relative config_path is resolved to absolute based on module file."""
        params = get_hydra_closure_params(hydra_module_relative.main)
        assert params.get("config_name") == "train.yaml"
        assert os.path.isabs(params["config_path"])
        expected = os.path.join(
            os.path.dirname(hydra_module_relative.__file__), "configs"
        )
        assert os.path.normpath(params["config_path"]) == os.path.normpath(expected)

    def test_query_update_no_hydra_cfg(self, hydra_module):
        """_query_update with no _hydra in config → uses closure only."""
        cfg = OC.create({"model": {"layers": 5}, "lr": 0.01})
        params = _query_update_hydra_params(hydra_module.main, hydra_module, cfg)
        assert params["config_name"] == "train.yaml"
        assert os.path.isabs(params["config_path"])

    def test_compose_and_merge(self, hydra_module, tmp_path):
        """Full compose + merge with real @hydra.main function."""
        workfolder = tmp_path / "OUT"
        workfolder.mkdir()

        cfg_routine = OC.create({"model": {"layers": 5}, "lr": 0.01})
        params = get_hydra_closure_params(hydra_module.main)

        cfg_result = _hydra_update_config(cfg_routine, workfolder, params, {})

        result = OC.to_container(cfg_result, resolve=True)
        assert result["model"]["layers"] == 5  # overridden by dervo
        assert result["model"]["name"] == "default_model"  # from hydra base
        assert result["lr"] == 0.01  # overridden by dervo

        assert (workfolder / "CONFIG.hydra.yml").exists()
        assert (workfolder / "CONFIG.hydra.internals.yml").exists()


# ===========================================================================
# Scenario 2: Plain function, _hydra in dervo config
# ===========================================================================


class TestHydraFromConfig:
    def test_query_update_with_hydra_cfg(self, plain_module, hydra_config_dir):
        """_hydra in config provides hydra params for a plain function."""
        cfg = OC.create(
            {
                "_hydra": {
                    "config_path": str(hydra_config_dir),
                    "config_name": "train.yaml",
                },
                "lr": 0.05,
            }
        )
        params = _query_update_hydra_params(plain_module.main, plain_module, cfg)
        assert params.get("config_name") == "train.yaml"
        assert params["config_path"] == str(hydra_config_dir)

    def test_hydra_cfg_overrides_closure(self, hydra_module, tmp_path):
        """_hydra config section overrides closure params."""
        other_dir = tmp_path / "other_configs"
        other_dir.mkdir()
        (other_dir / "other.yaml").write_text(
            yaml.dump(
                {"model": {"name": "other"}, "lr": 0.1}, default_flow_style=False
            )
        )

        cfg = OC.create(
            {
                "_hydra": {
                    "config_path": str(other_dir),
                    "config_name": "other.yaml",
                },
            }
        )
        params = _query_update_hydra_params(hydra_module.main, hydra_module, cfg)
        assert params["config_name"] == "other.yaml"
        assert params["config_path"] == str(other_dir)

    def test_compose_with_groups(self, hydra_config_dir_with_groups, tmp_path):
        """_hydra.groups selects config group files."""
        workfolder = tmp_path / "OUT"
        workfolder.mkdir()

        cfg_routine = OC.create({"lr": 0.05})
        hydra_params = {
            "config_path": str(hydra_config_dir_with_groups),
            "config_name": "train.yaml",
        }

        cfg_result = _hydra_update_config(
            cfg_routine, workfolder, hydra_params, {"model": "big"}
        )

        result = OC.to_container(cfg_result, resolve=True)
        assert result["model"]["name"] == "big_model"
        assert result["model"]["layers"] == 12
        assert result["lr"] == 0.05

    def test_compose_plain_func_with_hydra_config(
        self, plain_module, hydra_config_dir, tmp_path
    ):
        """Plain function + _hydra config → full hydra compose works."""
        workfolder = tmp_path / "OUT"
        workfolder.mkdir()

        cfg = OC.create(
            {
                "_hydra": {
                    "config_path": str(hydra_config_dir),
                    "config_name": "train.yaml",
                },
                "model": {"layers": 7},
            }
        )
        params = _query_update_hydra_params(plain_module.main, plain_module, cfg)
        cfg_routine = OC.masked_copy(cfg, [k for k in cfg if not k.startswith("_")])

        cfg_result = _hydra_update_config(cfg_routine, workfolder, params, {})

        result = OC.to_container(cfg_result, resolve=True)
        assert result["model"]["layers"] == 7
        assert result["model"]["name"] == "default_model"


# ===========================================================================
# Scenario 3: No hydra at all
# ===========================================================================


class TestNoHydra:
    def test_plain_function_empty_params(self, plain_module):
        """Plain function → closure returns empty dict."""
        params = get_hydra_closure_params(plain_module.main)
        assert params == {}

    def test_query_update_plain_no_config_name(self, plain_module):
        """Plain func + no _hydra → no config_name, hydra should not activate."""
        cfg = OC.create({"lr": 0.01})
        params = _query_update_hydra_params(plain_module.main, plain_module, cfg)
        assert not params.get("config_name")

    def test_add_args_signature_detection(self, plain_module):
        """Detect whether routine accepts add_args from real function signatures."""
        assert "add_args" not in inspect.signature(plain_module.main).parameters
        assert "add_args" in inspect.signature(plain_module.main_with_args).parameters


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEdgeCases:
    def test_hydra_config_singleton_set(self, hydra_module, tmp_path):
        """After _hydra_update_config, HydraConfig.get() works."""
        from hydra.core.hydra_config import HydraConfig

        workfolder = tmp_path / "OUT"
        workfolder.mkdir()

        cfg_routine = OC.create({"lr": 0.01})
        params = get_hydra_closure_params(hydra_module.main)

        _hydra_update_config(cfg_routine, workfolder, params, {})

        assert HydraConfig.initialized()
        hcfg = HydraConfig.get()
        assert str(workfolder) in hcfg.runtime.output_dir

    def test_unwrap_hydra_decorated(self, hydra_module):
        """Real @hydra.main function has __wrapped__ pointing to original."""
        func = hydra_module.main
        assert hasattr(func, "__wrapped__")
        assert "cfg" in inspect.signature(func.__wrapped__).parameters
