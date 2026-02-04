#!/usr/bin/env python3
from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Dict

from pymodbus.client import ModbusSerialClient

from util.pzem import read_pzem

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

def load_root_toml(path: Path) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore
    return tomllib.loads(path.read_text(encoding="utf-8"))

def make_modbus_client(cfg: dict) -> ModbusSerialClient:
    m = cfg["modbus"]
    kwargs: Dict[str, Any] = dict(
        port=str(m["port"]),
        baudrate=int(m.get("baudrate", 9600)),
        bytesize=int(m.get("bytesize", 8)),
        parity=str(m.get("parity", "N")),
        stopbits=int(m.get("stopbits", 1)),
        timeout=float(m.get("timeout_s", 1.0)),
    )
    try:
        sig = inspect.signature(ModbusSerialClient.__init__)
        if "method" in sig.parameters:
            kwargs["method"] = str(m.get("method", "rtu"))
    except Exception:
        pass
    return ModbusSerialClient(**kwargs)

def main() -> None:
    raw = load_root_toml(project_root() / "config.toml")
    client = make_modbus_client(raw)
    if not client.connect():
        raise SystemExit("Could not open serial port")

    try:
        # Broadcast write: holding reg 0x0002 = slave address
        # Broadcast address is 0; slave will NOT reply. 
        print("[INFO] Broadcasting: set slave address to 1 (no reply expected).")
        try:
            # Many pymodbus builds accept positional (addr, value, unit_id)
            client.write_register(0x0002, 1, 0)
        except TypeError:
            # Fallback for other signatures
            client.write_register(address=0x0002, value=1)

        # Give it a moment to apply
        import time
        time.sleep(0.2)

        print("[INFO] Trying to read unit 1...")
        r = read_pzem(client, unit_id=1, label="unit1", verbose=True)
        if r is None:
            print("[FAIL] Still no response at unit 1 after broadcast.")
        else:
            print("[OK] Unit responded at address 1.")
    finally:
        try:
            client.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
