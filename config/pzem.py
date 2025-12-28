# config/pzem.py
from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

from util.modbus import resolve_modbus_port

try:
    import tomllib  # py3.11+
except ModuleNotFoundError:  # pragma: no cover
    raise RuntimeError("Python 3.11+ required for tomllib. Use a newer Python or install tomli.")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOML_PATH = PROJECT_ROOT / "config.toml"


# -----------------------------
# Static / protocol constants
# -----------------------------
ABNORMAL_CODES = {
    0x01: "Illegal function",
    0x02: "Illegal address",
    0x03: "Illegal data",
    0x04: "Slave error",
}


# -----------------------------
# Safe TOML loading
# -----------------------------
def _load_root_toml(path: Path = DEFAULT_TOML_PATH) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        # Don’t crash import-time; fall back to defaults
        return {}


_RAW = _load_root_toml(DEFAULT_TOML_PATH)

# Sections (may be missing)
_MODBUS = dict(_RAW.get("modbus", {}) or {})
_SCAN = dict(_RAW.get("scan", {}) or {})
_PZEM = dict(_RAW.get("pzem", {}) or {})  # optional; you can add later


# -----------------------------
# Modbus serial settings
# (these names are imported by util/pzem.py, so keep them)
# -----------------------------
METHOD = str(_MODBUS.get("method", "rtu"))  # optional; poller already guards version differences

# UPDATED: choose from modbus.ports/port_candidates if present, else modbus.port
SERIAL_PORT = resolve_modbus_port(_MODBUS)

BAUDRATE = int(_MODBUS.get("baudrate", 9600))
BYTESIZE = int(_MODBUS.get("bytesize", 8))
STOPBITS = int(_MODBUS.get("stopbits", 1))
PARITY = str(_MODBUS.get("parity", "N"))
TIMEOUT = float(_MODBUS.get("timeout_s", 1.0))


# -----------------------------
# Device layout / defaults
# -----------------------------
def _as_int_key_dict(d: Any) -> dict[int, Any]:
    """
    TOML keys may come in as str or int.
    Normalize to {int: value}.
    """
    out: dict[int, Any] = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        try:
            out[int(k)] = v
        except Exception:
            continue
    return out


_NUM_FROM_PZEM = _PZEM.get("device_count", None)
if _NUM_FROM_PZEM is not None:
    NUM_PZEMS = int(_NUM_FROM_PZEM)
else:
    NUM_PZEMS = int(_SCAN.get("end_id", 1)) if int(_SCAN.get("end_id", 1)) >= 1 else 1

PZEM_IDS = list(range(1, NUM_PZEMS + 1))

DEFAULT_SHUNT_CODE = int(_PZEM.get("default_shunt_code", 0x0001))

_SHUNT_OVERRIDES_RAW = _PZEM.get("shunt_codes", {})
_SHUNT_OVERRIDES = _as_int_key_dict(_SHUNT_OVERRIDES_RAW)


def _warn_out_of_range_keys(raw: Any, label: str) -> None:
    if not isinstance(raw, dict):
        return
    out_of_range: list[int] = []
    for key in raw.keys():
        try:
            key_int = int(key)
        except Exception:
            continue
        if key_int not in PZEM_IDS:
            out_of_range.append(key_int)
    if out_of_range:
        unique = sorted(set(out_of_range))
        warnings.warn(
            f"{label} keys {unique} are outside configured PZEM_IDS {PZEM_IDS}. "
            "Update pzem.device_count or remove/adjust keys.",
            RuntimeWarning,
        )


_warn_out_of_range_keys(_SHUNT_OVERRIDES_RAW, "pzem.shunt_codes")

PZEM_SHUNT_CODES = {device_id: DEFAULT_SHUNT_CODE for device_id in PZEM_IDS}
for device_id, code in _SHUNT_OVERRIDES.items():
    if device_id in PZEM_SHUNT_CODES:
        try:
            PZEM_SHUNT_CODES[device_id] = int(code)
        except Exception:
            pass

_LABELS_RAW = _PZEM.get("labels", {})
_warn_out_of_range_keys(_LABELS_RAW, "pzem.labels")
LABELS = {str(k): str(v) for k, v in (_LABELS_RAW.items() if isinstance(_LABELS_RAW, dict) else [])}

SCAN_START_ID = int(_SCAN.get("start_id", 1))
SCAN_END_ID = int(_SCAN.get("end_id", max(NUM_PZEMS, 1)))
SCAN_VERBOSE = bool(_SCAN.get("verbose", False))
SCAN_TRY_PARAMS = bool(_SCAN.get("try_params", True))
SCAN_PER_ID_DELAY_S = float(_SCAN.get("per_id_delay_s", 0.6))
