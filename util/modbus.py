from __future__ import annotations

import os
import stat
from typing import Any, Mapping


def _port_usable(port: str) -> bool:
    """
    Conservative check: must exist and be a char device. Read/write access helps too.
    """
    try:
        st = os.stat(port)
        if not stat.S_ISCHR(st.st_mode):
            return False
        return os.access(port, os.R_OK | os.W_OK)
    except FileNotFoundError:
        return False
    except Exception:
        # If in doubt, allow connect() to decide.
        return True


def resolve_modbus_port(cfg: Mapping[str, Any]) -> str:
    """
    Backward compatible + safe:
      - If modbus.ports/port_candidates is present and non-empty, pick first usable in order.
      - Else use modbus.port (legacy).
    """
    if isinstance(cfg, Mapping) and isinstance(cfg.get("modbus"), Mapping):
        modbus_cfg: Mapping[str, Any] = cfg.get("modbus", {})
    elif isinstance(cfg, Mapping):
        modbus_cfg = cfg
    else:
        modbus_cfg = {}

    ports = modbus_cfg.get("ports") or modbus_cfg.get("port_candidates")
    if isinstance(ports, str):
        ports = [ports]
    if isinstance(ports, (list, tuple, set)) and ports:
        for port in ports:
            port_str = str(port)
            if _port_usable(port_str):
                return port_str
        # None usable -> fall back to legacy port

    return str(modbus_cfg.get("port", "/dev/ttyUSB0"))
