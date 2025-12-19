#!/usr/bin/env python3
from __future__ import annotations

import time
import os
import stat
from pathlib import Path
from typing import Dict, Any, Optional

from pymodbus.client import ModbusSerialClient

from config.influx import load_influx_settings
from util.influx import InfluxClient, pzem_reading_to_lp

# Use PZEM utilities (this is the point of util/pzem.py)
from util.pzem import read_pzem, make_modbus_client


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_root_toml(path: Path) -> dict:
    try:
        import tomllib  # py3.11+
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore
    return tomllib.loads(path.read_text(encoding="utf-8"))


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


def resolve_modbus_port(cfg: dict) -> str:
    """
    Backward compatible + safe:
      - If cfg['modbus']['ports'] is present and non-empty, pick first usable in order.
      - Else use cfg['modbus']['port'] (legacy).
    """
    m = dict(cfg.get("modbus", {}) or {})

    ports = m.get("ports") or m.get("port_candidates")
    if isinstance(ports, (list, tuple)) and ports:
        for p in ports:
            ps = str(p)
            if _port_usable(ps):
                return ps
        # None usable -> fall back to legacy port

    return str(m.get("port", "/dev/ttyUSB0"))


def _should_log_silence(streak: int) -> bool:
    if streak in (1, 5, 10, 30, 60, 120, 300):
        return True
    return streak > 0 and streak % 600 == 0


def modbus_socket_looks_broken(client: ModbusSerialClient) -> bool:
    ser = getattr(client, "socket", None)
    if ser is None:
        return True
    is_open = getattr(ser, "is_open", None)
    if isinstance(is_open, bool) and not is_open:
        return True
    return False


def _set_util_port(selected_port: str) -> None:
    """
    Light-touch integration:
    util.pzem.make_modbus_client() uses config.pzem.SERIAL_PORT.
    We patch it at runtime so poll_to_influx can select /dev/serial0 when available.
    """
    import config.pzem as pzem_cfg  # runtime import so patching affects util.pzem
    pzem_cfg.SERIAL_PORT = str(selected_port)


def connect_modbus(raw_cfg: dict) -> ModbusSerialClient:
    """
    Choose port, patch util config, create client via util.pzem.make_modbus_client(),
    then connect.
    """
    selected_port = resolve_modbus_port(raw_cfg)
    _set_util_port(selected_port)

    client = make_modbus_client()
    if not client.connect():
        if selected_port in ("/dev/serial0", "/dev/ttyAMA0", "/dev/ttyS0"):
            print("[HINT] If you're on Raspberry Pi, make sure serial console/login shell is disabled and UART is enabled.")
        raise RuntimeError(f"Could not open Modbus port {selected_port}")
    print(f"[INFO] Modbus connected on port: {selected_port}")
    return client


def reconnect_modbus(
    raw_cfg: dict,
    old_client: Optional[ModbusSerialClient],
    *,
    attempts: int,
    base_delay_s: float,
) -> ModbusSerialClient:
    """
    Close + reconnect with retries, re-resolving port each time.
    Uses util.pzem.make_modbus_client() for actual client creation.
    """
    if old_client is not None:
        try:
            old_client.close()
        except Exception:
            pass

    last_err: Optional[Exception] = None

    for i in range(1, attempts + 1):
        selected_port = resolve_modbus_port(raw_cfg)
        _set_util_port(selected_port)

        client = make_modbus_client()
        try:
            ok = client.connect()
        except Exception as e:
            ok = False
            last_err = e

        if ok:
            print(f"[INFO] Modbus reconnect succeeded on attempt {i} (port={selected_port}).")
            return client

        delay = min(base_delay_s * i, 10.0)
        if last_err is not None:
            print(f"[WARN] Modbus reconnect attempt {i}/{attempts} failed (port={selected_port}): {last_err}. Sleeping {delay:.1f}s.")
        else:
            print(f"[WARN] Modbus reconnect attempt {i}/{attempts} failed (port={selected_port}, connect()=False). Sleeping {delay:.1f}s.")
        time.sleep(delay)

    raise SystemExit(f"Modbus reconnect failed after {attempts} attempts. Last error: {last_err}")


def main() -> None:
    root = project_root()
    toml_path = root / "config.toml"
    raw_cfg = load_root_toml(toml_path)

    influx_settings = load_influx_settings(toml_path)
    influx = InfluxClient(influx_settings)

    poller = raw_cfg.get("poller", {})
    poll_interval = float(poller.get("interval_s", influx_settings.poll_interval_s))
    measurement = str(poller.get("measurement", influx_settings.measurement))

    silent_backoff_s = float(poller.get("silent_backoff_s", 0.0))
    influx_retry_delay_s = float(poller.get("influx_retry_delay_s", 2.0))

    hard_reconnect_after_iters = int(poller.get("modbus_reconnect_after_hard_error_iters", 3))
    soft_reconnect_after_iters = int(poller.get("modbus_reconnect_after_all_silent_iters", 0))
    modbus_reconnect_attempts = int(poller.get("modbus_reconnect_attempts", 5))
    modbus_reconnect_base_delay_s = float(poller.get("modbus_reconnect_base_delay_s", 1.0))

    # PZEM config
    pzem_cfg = raw_cfg.get("pzem", {})
    device_count = int(pzem_cfg.get("device_count", 1))
    labels = pzem_cfg.get("labels", {})
    unit_ids = list(range(1, device_count + 1))

    global_tags: Dict[str, str] = dict(raw_cfg.get("tags", {}) or {})

    try:
        print("Influx health:", influx.health())
    except Exception as e:
        print(f"[WARN] Influx health check failed at startup: {e}")

    client: Optional[ModbusSerialClient] = None
    try:
        client = connect_modbus(raw_cfg)

        silent_streak: Dict[int, int] = {uid: 0 for uid in unit_ids}

        consecutive_hard_error_iters = 0
        consecutive_all_silent_iters = 0
        ever_had_success = False

        while True:
            hard_error_this_iter = modbus_socket_looks_broken(client)

            lines: list[str] = []
            any_success = False

            for unit_id in unit_ids:
                label = labels.get(str(unit_id), f"unit{unit_id}")

                try:
                    reading = read_pzem(client, unit_id=unit_id, label=label, verbose=False)
                except Exception as e:
                    hard_error_this_iter = True
                    silent_streak[unit_id] += 1
                    if _should_log_silence(silent_streak[unit_id]):
                        print(f"[INFO] Unit {unit_id} ({label}) read exception (treated as no data): {e}")
                    continue

                if reading is None:
                    silent_streak[unit_id] += 1
                    if _should_log_silence(silent_streak[unit_id]):
                        print(f"[INFO] Unit {unit_id} ({label}) no response (likely <~7V); skipping write.")
                    continue

                any_success = True
                ever_had_success = True
                silent_streak[unit_id] = 0

                fields: Dict[str, Any] = {
                    "voltage": float(reading.voltage),
                    "current": float(reading.current),
                    "power": float(reading.power),
                    "energy_wh": int(reading.energy_wh),
                    "raw_hv": int(reading.raw_hv),
                    "raw_lv": int(reading.raw_lv),
                }

                tags = {
                    **global_tags,
                    "label": str(label),
                }

                lines.append(
                    pzem_reading_to_lp(
                        measurement=measurement,
                        unit_id=unit_id,
                        fields=fields,
                        tags=tags,
                    )
                )

            if any_success:
                consecutive_hard_error_iters = 0
                consecutive_all_silent_iters = 0
            else:
                consecutive_all_silent_iters += 1
                if hard_error_this_iter:
                    consecutive_hard_error_iters += 1
                else:
                    consecutive_hard_error_iters = 0

            if hard_reconnect_after_iters > 0 and consecutive_hard_error_iters >= hard_reconnect_after_iters:
                print(
                    f"[WARN] Detected hard Modbus failures for {consecutive_hard_error_iters} consecutive iterations; "
                    f"attempting Modbus reconnect..."
                )
                client = reconnect_modbus(
                    raw_cfg,
                    client,
                    attempts=modbus_reconnect_attempts,
                    base_delay_s=modbus_reconnect_base_delay_s,
                )
                consecutive_hard_error_iters = 0
                consecutive_all_silent_iters = 0

            if (
                soft_reconnect_after_iters > 0
                and ever_had_success
                and consecutive_all_silent_iters >= soft_reconnect_after_iters
                and consecutive_hard_error_iters == 0
            ):
                print(
                    f"[WARN] All units silent for {consecutive_all_silent_iters} iterations (soft silence); "
                    f"attempting Modbus reconnect (optional policy)..."
                )
                client = reconnect_modbus(
                    raw_cfg,
                    client,
                    attempts=modbus_reconnect_attempts,
                    base_delay_s=modbus_reconnect_base_delay_s,
                )
                consecutive_hard_error_iters = 0
                consecutive_all_silent_iters = 0

            if lines:
                try:
                    influx.write_lp("\n".join(lines) + "\n")
                except Exception as e:
                    print(f"[WARN] Influx write failed (will continue): {e}")
                    time.sleep(influx_retry_delay_s)

            if (not any_success) and silent_backoff_s > 0:
                time.sleep(silent_backoff_s)

            time.sleep(poll_interval)

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
