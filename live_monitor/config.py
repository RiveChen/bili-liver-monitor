# -*- coding: utf-8 -*-
"""Configuration management with Pydantic + YAML + local overrides."""

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

log = logging.getLogger("config")


# ── Pydantic models ──────────────────────────────────────────────


class BilibiliMonitorConfig(BaseModel):
    """Bilibili live monitor configuration."""

    uid_list: list[int] = Field(default_factory=list)
    notify_live_end: bool = True
    poll_interval: int = 30


class MonitorConfig(BaseModel):
    """Monitor section configuration."""

    bilibili: BilibiliMonitorConfig = Field(default_factory=BilibiliMonitorConfig)


class NapCatPusherConfig(BaseModel):
    """NapCatQQ pusher configuration."""

    api_url: str = "http://127.0.0.1:3000"
    token: str = ""
    user_id: int = Field(default=0, ge=0)
    group_ids: list[int] = Field(default_factory=list)
    at_qq: str = ""


class PusherConfig(BaseModel):
    """Pusher section configuration."""

    napcat: NapCatPusherConfig = Field(default_factory=NapCatPusherConfig)


class AppConfig(BaseModel):
    """Root configuration model."""

    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    pusher: PusherConfig = Field(default_factory=PusherConfig)
    log_level: str = "INFO"


# ── Helpers ──────────────────────────────────────────────────────


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge two dicts: override values take precedence."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: str = "config.yml") -> AppConfig:
    """Load and validate configuration.

    1. Load config.yml (committed template with placeholder values)
    2. If config.local.yml exists, deep-merge it on top
       (local overrides, .gitignored, never committed)
    3. Validate with Pydantic

    Args:
        path: Path to the main config file.

    Returns:
        Validated AppConfig instance.
    """
    config_path = Path(path)

    # Step 1: Load base config
    if not config_path.exists():
        log.warning("config file %s not found, using defaults", path)
        raw: dict = {}
    else:
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    # Step 2: Merge local overrides
    local_path = config_path.with_name(config_path.stem + ".local" + config_path.suffix)
    if local_path.exists():
        log.info("Loading local config overrides from %s", local_path)
        with open(local_path, encoding="utf-8") as f:
            local_raw = yaml.safe_load(f) or {}
        raw = _deep_merge(raw, local_raw)

    # Step 3: Validate with Pydantic
    try:
        config = AppConfig.model_validate(raw)
    except Exception as e:
        log.error("Configuration validation failed: %s", e)
        raise

    return config
