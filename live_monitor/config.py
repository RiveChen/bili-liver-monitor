# -*- coding: utf-8 -*-
"""Configuration management with dataclasses + YAML + local overrides.

Replaces the previous Pydantic-based implementation to avoid the
Rust-compilation dependency (pydantic-core) on platforms like Android/Termux.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)


# ── Dataclass models ────────────────────────────────────────────


@dataclass
class BilibiliMonitorConfig:
    """Bilibili live & dynamic monitor configuration."""

    uid_list: list[int] = field(default_factory=list)
    notify_live_end: bool = True
    poll_interval: int = 30

    # Dynamic (动态) monitoring
    notify_dynamic: bool = True
    dynamic_poll_interval: int = 60
    skip_forward: bool = True
    cookie: str = ""


@dataclass
class MonitorConfig:
    """Monitor section configuration."""

    bilibili: BilibiliMonitorConfig = field(default_factory=BilibiliMonitorConfig)


@dataclass
class NapCatPusherConfig:
    """NapCatQQ pusher configuration."""

    api_url: str = "http://127.0.0.1:3000"
    token: str = ""
    user_id: int = 0
    group_ids: list[int] = field(default_factory=list)
    at_qq: str = ""


@dataclass
class ListenerConfig:
    """NapCatQQ event listener configuration."""

    bot_qq: int = 0
    ws_url: str = ""
    allowed_groups: list[int] = field(default_factory=list)


@dataclass
class PusherConfig:
    """Pusher section configuration."""

    napcat: NapCatPusherConfig = field(default_factory=NapCatPusherConfig)


@dataclass
class AppConfig:
    """Root configuration model."""

    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    pusher: PusherConfig = field(default_factory=PusherConfig)
    listener: ListenerConfig = field(default_factory=ListenerConfig)
    log_level: str = "INFO"


# ── Dict → dataclass builder ────────────────────────────────────


def _from_dict(cls: type, data: dict) -> Any:
    """Recursively build a dataclass instance from a (possibly nested) dict.

    Performs basic type validation – raises ``TypeError`` when a value's type
    does not match the declared field type.
    """
    import dataclasses
    from typing import cast

    if not dataclasses.is_dataclass(cls):
        return data

    fields_by_name = {f.name: f for f in dataclasses.fields(cls)}
    kwargs: dict = {}

    for name, fdef in fields_by_name.items():
        if name not in data:
            continue  # rely on the field's default / default_factory

        value = data[name]
        target = fdef.type

        origin = getattr(target, "__origin__", None)
        args = getattr(target, "__args__", ())

        # Narrow type: is_dataclass(target) ensures it's a class, not an instance
        if dataclasses.is_dataclass(target):
            target_cls = cast(type, target)
            # Nested dataclass
            if isinstance(value, dict):
                kwargs[name] = _from_dict(target_cls, value)
            else:
                kwargs[name] = value
        elif origin is list and args and dataclasses.is_dataclass(args[0]):
            # list[SomeDataclass]
            elem_cls = cast(type, args[0])
            if isinstance(value, list):
                kwargs[name] = [_from_dict(elem_cls, item) for item in value]
            else:
                kwargs[name] = value
        else:
            # Scalar / simple type – validate.
            # Use the origin (e.g. ``list`` for ``list[int]``) when dealing
            # with parameterised generics, since ``isinstance()`` cannot
            # accept e.g. ``list[int]`` directly.
            if origin is not None:
                check_type: type = cast(type, origin)
            else:
                check_type = cast(type, target)
            if not isinstance(value, check_type):
                raise TypeError(
                    f"Field '{name}' expects {target}, "
                    f"got {type(value).__name__}: {value!r}"
                )
            kwargs[name] = value

    return cls(**kwargs)


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
    3. Build an :class:`AppConfig` dataclass from the merged dict

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
    local_path = config_path.with_name(
        config_path.stem + ".local" + config_path.suffix
    )
    if local_path.exists():
        log.info("Loading local config overrides from %s", local_path)
        with open(local_path, encoding="utf-8") as f:
            local_raw = yaml.safe_load(f) or {}
        raw = _deep_merge(raw, local_raw)

    # Step 3: Build dataclass (with type validation)
    try:
        config = _from_dict(AppConfig, raw)
    except TypeError as e:
        log.error("Configuration validation failed: %s", e)
        raise

    return config
