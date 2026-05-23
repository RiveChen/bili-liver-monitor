"""Tests for config module: loading, deep-merge, validation."""

from pathlib import Path

import pytest
import yaml

from live_monitor.config import AppConfig, _deep_merge, load_config


# ── _deep_merge pure function tests ─────────────────────────────


class TestDeepMerge:
    def test_both_empty(self) -> None:
        assert _deep_merge({}, {}) == {}

    def test_override_scalar(self) -> None:
        base = {"a": 1, "b": 2}
        override = {"a": 99}
        assert _deep_merge(base, override) == {"a": 99, "b": 2}

    def test_nested_deep_merge(self) -> None:
        base = {
            "monitor": {
                "bilibili": {"uid_list": [1], "poll_interval": 30},
            }
        }
        override = {
            "monitor": {
                "bilibili": {"uid_list": [2]},
            }
        }
        result = _deep_merge(base, override)
        assert result == {
            "monitor": {
                "bilibili": {"uid_list": [2], "poll_interval": 30},
            }
        }

    def test_override_none_overwrites(self) -> None:
        base = {"a": {"b": 1}}
        override = {"a": None}
        result = _deep_merge(base, override)
        assert result == {"a": None}

    def test_new_key_in_override(self) -> None:
        base = {"a": 1}
        override = {"b": 2}
        assert _deep_merge(base, override) == {"a": 1, "b": 2}

    def test_empty_override_does_nothing(self) -> None:
        base = {"a": 1, "b": {"c": 2}}
        assert _deep_merge(base, {}) == base

    def test_empty_base_gets_override(self) -> None:
        assert _deep_merge({}, {"a": 1}) == {"a": 1}

    def test_does_not_mutate_inputs(self) -> None:
        base = {"a": {"b": 1}}
        override = {"a": {"c": 2}}
        result = _deep_merge(base, override)
        assert result == {"a": {"b": 1, "c": 2}}
        # Original dicts should not have been mutated
        assert base == {"a": {"b": 1}}
        assert override == {"a": {"c": 2}}


# ── load_config tests ───────────────────────────────────────────


class TestLoadConfig:
    def test_file_not_found_returns_defaults(self) -> None:
        """When config file doesn't exist, should return AppConfig defaults."""
        config = load_config("/nonexistent/path/config.yml")
        assert isinstance(config, AppConfig)
        assert config.monitor.bilibili.uid_list == []
        assert config.monitor.bilibili.poll_interval == 30
        assert config.pusher.napcat.api_url == "http://127.0.0.1:3000"
        assert config.log_level == "INFO"

    def test_load_basic_config(self, config_path: Path) -> None:
        """Load a valid config.yml and verify field mapping."""
        config = load_config(str(config_path))
        assert config.monitor.bilibili.uid_list == [12345]
        assert config.monitor.bilibili.poll_interval == 30
        assert config.monitor.bilibili.notify_live_end is True
        assert config.pusher.napcat.api_url == "http://127.0.0.1:3000"
        assert config.pusher.napcat.user_id == 98765
        assert config.pusher.napcat.group_ids == [556677]
        assert config.log_level == "INFO"

    def test_local_overrides_merged(self, config_with_local: Path) -> None:
        """config.local.yml should override config.yml values via deep merge."""
        config = load_config(str(config_with_local))
        # uid_list overridden by local
        assert config.monitor.bilibili.uid_list == [999999]
        # user_id overridden by local
        assert config.pusher.napcat.user_id == 111111
        # group_ids not overridden, should stay from base
        assert config.pusher.napcat.group_ids == [556677]
        # poll_interval unchanged
        assert config.monitor.bilibili.poll_interval == 30

    def test_empty_yaml_returns_defaults(self, config_dir: Path) -> None:
        """An empty config.yml should result in AppConfig defaults."""
        p = config_dir / "config.yml"
        p.write_text("", encoding="utf-8")
        config = load_config(str(p))
        assert config.monitor.bilibili.uid_list == []
        assert config.log_level == "INFO"

    def test_invalid_yaml_raises_error(self, config_dir: Path) -> None:
        """Malformed YAML should raise yaml.YAMLError."""
        p = config_dir / "config.yml"
        p.write_text("{invalid: [unbalanced", encoding="utf-8")
        with pytest.raises(yaml.YAMLError):
            load_config(str(p))

    def test_invalid_type_raises_validation_error(self, config_dir: Path) -> None:
        """Config with wrong types should raise TypeError from _from_dict."""
        p = config_dir / "config.yml"
        p.write_text(
            """
monitor:
  bilibili:
    uid_list: "not-a-list"
""",
            encoding="utf-8",
        )
        with pytest.raises(TypeError):
            load_config(str(p))

    def test_local_does_not_exist_uses_base_only(self, config_path: Path) -> None:
        """When config.local.yml doesn't exist, only config.yml is used."""
        # Ensure no local file exists
        local = config_path.with_name(config_path.stem + ".local" + config_path.suffix)
        if local.exists():
            local.unlink()
        config = load_config(str(config_path))
        assert config.monitor.bilibili.uid_list == [12345]
        assert config.pusher.napcat.user_id == 98765

    def test_partial_local_override(
        self, config_dir: Path, sample_config_yaml: str
    ) -> None:
        """Local file overriding only one field should leave others intact."""
        base_p = config_dir / "config.yml"
        base_p.write_text(sample_config_yaml, encoding="utf-8")

        local_p = config_dir / "config.local.yml"
        local_p.write_text(
            """
log_level: DEBUG
""",
            encoding="utf-8",
        )
        config = load_config(str(base_p))
        assert config.log_level == "DEBUG"
        assert config.monitor.bilibili.uid_list == [12345]
        assert config.pusher.napcat.user_id == 98765
