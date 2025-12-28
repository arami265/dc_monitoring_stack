from __future__ import annotations

import os
import stat
from collections.abc import Iterable, Mapping
from typing import Any


def _port_usable(p: str) -> bool:
    try:
        st = os.stat(p)
        if not stat.S_ISCHR(st.st_mode):
            return False
        return os.access(p, os.R_OK | os.W_OK)
    except FileNotFoundError:
        return False
    except Exception:
        # If in doubt, allow connect() to decide.
        return True


def _as_port_candidates(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, dict):
        return []
    if isinstance(raw, Iterable):
        return [str(p) for p in raw]
    return []


def resolve_modbus_port(cfg: Mapping[str, Any] | None) -> str:
    """
    Backward compatible + safe:
      - If cfg['modbus']['ports'] or cfg['modbus']['port_candidates'] is present and non-empty,
        pick first usable in order.
      - Else use cfg['modbus']['port'] (legacy).
    """
    raw_cfg = cfg or {}
    if "modbus" in raw_cfg and isinstance(raw_cfg.get("modbus"), Mapping):
        modbus_cfg = dict(raw_cfg.get("modbus") or {})
    else:
        modbus_cfg = dict(raw_cfg)

    ports_raw = modbus_cfg.get("ports") or modbus_cfg.get("port_candidates")
    port_candidates = _as_port_candidates(ports_raw)
    if port_candidates:
        for port in port_candidates:
            if _port_usable(port):
                return port
        # None usable -> fall back to legacy port

    return str(modbus_cfg.get("port", "/dev/ttyUSB0"))
