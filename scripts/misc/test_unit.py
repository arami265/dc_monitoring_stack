#!/usr/bin/env python3
from __future__ import annotations

import time
from typing import Optional

from util.pzem import pzem_client, read_pzem

UNIT_ADDRESS = 1
RETRIES = 3
RETRY_DELAY_S = 0.5

# Optional: your PZEMs seem to require the sense side to be >~7V to respond
MIN_EXPECTED_VOLTAGE_FOR_COMMS = 7.0


def try_read(client, unit_id: int) -> Optional[object]:
    last = None
    for attempt in range(1, RETRIES + 1):
        last = read_pzem(client, unit_id, label=f"unit{unit_id}", verbose=True)
        if last is not None:
            return last
        print(f"[WARN] No response from unit {unit_id} (attempt {attempt}/{RETRIES}).")
        time.sleep(RETRY_DELAY_S)
    return None


def main() -> None:
    with pzem_client() as client:
        # Helpful context
        port = getattr(client, "port", None) or getattr(client, "comm_params", None) or "(unknown)"
        print(f"[INFO] Connected. Testing Modbus unit {UNIT_ADDRESS}. Port info: {port}")

        reading = try_read(client, UNIT_ADDRESS)

    if reading is None:
        print(
            f"[FAIL] No response from unit {UNIT_ADDRESS} after {RETRIES} attempts.\n"
            f"       If this is a PZEM-017, double-check the sense side is powered (>~{MIN_EXPECTED_VOLTAGE_FOR_COMMS}V),\n"
            f"       and that A/B/GND + termination/biasing are correct."
        )
        raise SystemExit(2)

    # Basic sanity checks
    v = float(reading.voltage)
    i = float(reading.current)
    p = float(reading.power)

    print("\n[INFO] Sanity checks:")
    if v < MIN_EXPECTED_VOLTAGE_FOR_COMMS:
        print(f"  - Voltage is {v:.2f}V (<~{MIN_EXPECTED_VOLTAGE_FOR_COMMS}V). Comms may be flaky on this model.")
    else:
        print(f"  - Voltage looks plausible: {v:.2f}V")

    # Rough power consistency (very loose; small currents are noisy)
    approx_p = v * i
    if (abs(approx_p) > 1.0) and (abs(p - approx_p) / abs(approx_p) > 0.5):
        print(f"  - Power mismatch warning: reported {p:.2f}W vs V*I≈{approx_p:.2f}W (could be normal at tiny currents).")
    else:
        print(f"  - Power roughly consistent: reported {p:.2f}W vs V*I≈{approx_p:.2f}W")

    print("\n[OK] Read succeeded.")


if __name__ == "__main__":
    main()
