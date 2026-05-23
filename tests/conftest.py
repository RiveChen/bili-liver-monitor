"""Shared fixtures for bili-liver-monitor tests."""

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest


# ── Config fixtures ─────────────────────────────────────────────


@pytest.fixture
def sample_config_yaml() -> str:
    """A minimal valid config.yml content (public template)."""
    return """
monitor:
  bilibili:
    uid_list:
      - 12345
    notify_live_end: true
    poll_interval: 30

pusher:
  napcat:
    api_url: "http://127.0.0.1:3000"
    token: ""
    user_id: 98765
    group_ids:
      - 556677
    at_qq: ""

log_level: INFO
"""


@pytest.fixture
def sample_local_yaml() -> str:
    """A config.local.yml with overrides."""
    return """
monitor:
  bilibili:
    uid_list:
      - 999999

pusher:
  napcat:
    user_id: 111111
"""


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for config files."""
    d = tmp_path / "configs"
    d.mkdir()
    return d


@pytest.fixture
def config_path(config_dir: Path, sample_config_yaml: str) -> Path:
    """Write sample_config_yaml to a temp config.yml and return its path."""
    p = config_dir / "config.yml"
    p.write_text(sample_config_yaml, encoding="utf-8")
    return p


@pytest.fixture
def config_with_local(
    config_path: Path,
    sample_local_yaml: str,
) -> Path:
    """Return config_path after also writing config.local.yml alongside it."""
    local_path = config_path.with_name(config_path.stem + ".local" + config_path.suffix)
    local_path.write_text(sample_local_yaml, encoding="utf-8")
    return config_path


# ── Monitor fixtures ───────────────────────────────────────────


@pytest.fixture
def mock_callbacks() -> dict[str, Callable[..., Awaitable[None]]]:
    """Return placeholder async callbacks that record calls."""

    async def noop(*args: Any, **kwargs: Any) -> None:
        pass

    return {
        "on_live_start": noop,
        "on_live_end": noop,
    }


# ── NapCat fixture helpers ─────────────────────────────────────


@pytest.fixture
def napcat_api_base() -> str:
    return "http://127.0.0.1:3000"
