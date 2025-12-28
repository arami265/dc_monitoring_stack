from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from typing import Optional, Iterator, Literal, Any, Dict, List
import inspect
import time

from pymodbus.client import ModbusSerialClient

from config.behavior import VERBOSE_READ_DEFAULT
from config.pzem import (
    METHOD, SERIAL_PORT, BAUDRATE, BYTESIZE,
    STOPBITS, PARITY, TIMEOUT, ABNORMAL_CODES,
)

# ---------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------

class PzemError(Exception):
    """Base exception for PZEM utility operations."""


class PzemModbusError(PzemError):
    """Raised when a Modbus RTU exchange fails or returns an exception frame."""
    def __init__(self, message: str, *, unit_id: int, detail: Optional[str] = None):
        super().__init__(message)
        self.unit_id = unit_id
        self.detail = detail


class PzemRawCommandError(PzemError):
    """Raised when a non-standard raw PZEM command fails."""
    def __init__(self, message: str, *, unit_id: Optional[int] = None, abnormal_code: Optional[int] = None):
        super().__init__(message)
        self.unit_id = unit_id
        self.abnormal_code = abnormal_code


# ---------------------------------------------------------------------
# ID validation + serial access
# ---------------------------------------------------------------------

def _require_unit_id(unit_id: int) -> None:
    if not (1 <= int(unit_id) <= 247):
        raise ValueError(f"Invalid Modbus unit id {unit_id}. Use 1..247.")


def _get_serial_port_from_client(client: ModbusSerialClient):
    """
    pymodbus serial client exposes the underlying pyserial object as client.socket (after connect()).
    """
    ser = getattr(client, "socket", None)
    if ser is None or not hasattr(ser, "write") or not hasattr(ser, "read"):
        raise PzemError(
            "Could not access underlying serial port from ModbusSerialClient "
            "(expected client.socket after client.connect())."
        )
    return ser


def _best_effort_flush(ser) -> None:
    try:
        if hasattr(ser, "reset_input_buffer"):
            ser.reset_input_buffer()
        if hasattr(ser, "reset_output_buffer"):
            ser.reset_output_buffer()
    except Exception:
        pass


# ---------------------------------------------------------------------
# CRC (Modbus RTU)
# ---------------------------------------------------------------------

CrcOrder = Literal["little", "big"]


def crc16_modbus(data: bytes) -> int:
    """
    CRC16-Modbus (poly 0xA001), returns 0..65535.
    """
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if (crc & 1) else (crc >> 1)
    return crc & 0xFFFF


def append_crc(data: bytes, *, crc_order: CrcOrder = "little") -> bytes:
    """
    Append CRC bytes; Modbus RTU on-wire is low byte then high byte ("little").
    """
    crc = crc16_modbus(data)
    lo = crc & 0xFF
    hi = (crc >> 8) & 0xFF
    return data + (bytes([lo, hi]) if crc_order == "little" else bytes([hi, lo]))


def _check_crc(frame: bytes) -> bool:
    if len(frame) < 3:
        return False
    data = frame[:-2]
    rx_lo = frame[-2]
    rx_hi = frame[-1]
    rx = rx_lo | (rx_hi << 8)
    calc = crc16_modbus(data)
    return rx == calc


def _read_exact(ser, n: int, timeout_s: float) -> bytes:
    """
    Read up to n bytes, returning fewer on timeout.
    Uses a monotonic deadline in addition to pyserial timeout for robustness.
    """
    deadline = time.monotonic() + float(timeout_s)
    buf = bytearray()
    while len(buf) < n and time.monotonic() < deadline:
        chunk = ser.read(n - len(buf))
        if chunk:
            buf.extend(chunk)
        else:
            # ser.read() timed out; break if we're past deadline
            if time.monotonic() >= deadline:
                break
    return bytes(buf)


# ---------------------------------------------------------------------
# Raw Modbus RTU helpers (stable across pymodbus versions)
# ---------------------------------------------------------------------

def _rtu_read_registers(
    ser,
    *,
    unit_id: int,
    function_code: int,  # 0x03 holding, 0x04 input
    start_addr: int,
    count: int,
    timeout_s: float,
) -> List[int]:
    """
    Send an RTU Read Registers request and return list of 16-bit regs.
    Raises PzemModbusError on no response / exception / CRC / framing issues.
    """
    _require_unit_id(unit_id)

    if function_code not in (0x03, 0x04):
        raise ValueError("function_code must be 0x03 or 0x04")

    if not (0 <= start_addr <= 0xFFFF) or not (1 <= count <= 0x7D):  # Modbus typical max 125 regs
        raise ValueError("Invalid start_addr/count")

    req = bytes([
        unit_id,
        function_code,
        (start_addr >> 8) & 0xFF, start_addr & 0xFF,
        (count >> 8) & 0xFF, count & 0xFF,
    ])
    req = append_crc(req)

    _best_effort_flush(ser)
    ser.write(req)
    if hasattr(ser, "flush"):
        ser.flush()

    # Read 3 bytes first: [unit][fc][bytecount] OR [unit][fc|0x80][exc]
    hdr = _read_exact(ser, 3, timeout_s)
    if len(hdr) < 3:
        raise PzemModbusError("No response (header timeout)", unit_id=unit_id)

    if hdr[0] != unit_id:
        raise PzemModbusError(f"Unexpected responder 0x{hdr[0]:02X}", unit_id=unit_id)

    fc = hdr[1]
    if fc == (function_code | 0x80):
        # Exception frame: [unit][fc|0x80][exc][crc][crc]
        tail = _read_exact(ser, 2, timeout_s)
        frame = hdr + tail
        if len(frame) < 5:
            raise PzemModbusError("Truncated exception response", unit_id=unit_id, detail=frame.hex(" "))
        if not _check_crc(frame):
            raise PzemModbusError("Bad CRC on exception response", unit_id=unit_id, detail=frame.hex(" "))
        exc = frame[2]
        msg = ABNORMAL_CODES.get(exc, f"Unknown abnormal code 0x{exc:02X}")
        raise PzemModbusError(f"Modbus exception 0x{exc:02X} ({msg})", unit_id=unit_id, detail=frame.hex(" "))

    if fc != function_code:
        raise PzemModbusError(
            f"Unexpected function code 0x{fc:02X} (expected 0x{function_code:02X})",
            unit_id=unit_id,
            detail=hdr.hex(" "),
        )

    bytecount = hdr[2]
    expected_data_bytes = 2 * count
    if bytecount != expected_data_bytes:
        # still try to read whatever device claims, but validate after
        pass

    # Remaining: data bytes + CRC(2)
    tail = _read_exact(ser, int(bytecount) + 2, timeout_s)
    frame = hdr + tail

    if len(frame) < 3 + bytecount + 2:
        raise PzemModbusError("Truncated data response", unit_id=unit_id, detail=frame.hex(" "))

    if not _check_crc(frame):
        raise PzemModbusError("Bad CRC on data response", unit_id=unit_id, detail=frame.hex(" "))

    data = frame[3:3 + bytecount]
    if len(data) % 2 != 0:
        raise PzemModbusError("Odd data length in response", unit_id=unit_id, detail=frame.hex(" "))

    regs: List[int] = []
    for i in range(0, len(data), 2):
        regs.append((data[i] << 8) | data[i + 1])

    # If device returned fewer regs than requested, that's a protocol violation for our use.
    if len(regs) < count:
        raise PzemModbusError(
            f"Device returned {len(regs)} registers (expected {count})",
            unit_id=unit_id,
            detail=frame.hex(" "),
        )

    return regs[:count]


def _rtu_write_single_register(
    ser,
    *,
    unit_id: int,
    addr: int,
    value: int,
    timeout_s: float,
) -> None:
    """
    FC06 write single holding register. Expects echo reply.
    Raises PzemModbusError on failure.
    """
    _require_unit_id(unit_id)

    if not (0 <= addr <= 0xFFFF) or not (0 <= value <= 0xFFFF):
        raise ValueError("Invalid addr/value")

    req = bytes([
        unit_id,
        0x06,
        (addr >> 8) & 0xFF, addr & 0xFF,
        (value >> 8) & 0xFF, value & 0xFF,
    ])
    req = append_crc(req)

    _best_effort_flush(ser)
    ser.write(req)
    if hasattr(ser, "flush"):
        ser.flush()

    resp = _read_exact(ser, 8, timeout_s)
    if len(resp) < 5:
        raise PzemModbusError("No response / truncated write reply", unit_id=unit_id, detail=resp.hex(" "))

    # Exception reply for FC06 would be 5 bytes
    if len(resp) >= 5 and resp[1] == (0x06 | 0x80):
        # Ensure we have full 5
        if len(resp) < 5:
            resp += _read_exact(ser, 5 - len(resp), timeout_s)
        if not _check_crc(resp[:5]):
            raise PzemModbusError("Bad CRC on exception write reply", unit_id=unit_id, detail=resp.hex(" "))
        exc = resp[2]
        msg = ABNORMAL_CODES.get(exc, f"Unknown abnormal code 0x{exc:02X}")
        raise PzemModbusError(f"Modbus exception 0x{exc:02X} ({msg})", unit_id=unit_id, detail=resp.hex(" "))

    if len(resp) < 8:
        # Try to read remaining bytes if partially received
        resp += _read_exact(ser, 8 - len(resp), timeout_s)

    if len(resp) != 8:
        raise PzemModbusError("Truncated FC06 echo reply", unit_id=unit_id, detail=resp.hex(" "))

    if not _check_crc(resp):
        raise PzemModbusError("Bad CRC on FC06 echo reply", unit_id=unit_id, detail=resp.hex(" "))

    if resp != req:
        raise PzemModbusError(
            "FC06 reply mismatch (expected echo)",
            unit_id=unit_id,
            detail=f"TX={req.hex(' ')} RX={resp.hex(' ')}",
        )


# ---------------------------------------------------------------------
# Client creation + context manager
# ---------------------------------------------------------------------

def _rtu_framer_kw() -> Dict[str, Any]:
    # Not required for our raw RTU, but harmless if supported
    try:
        from pymodbus.framer import FramerType  # type: ignore
        return {"framer": FramerType.RTU}
    except Exception:
        return {}


def make_modbus_client() -> ModbusSerialClient:
    """
    Create a ModbusSerialClient using shared config values.
    Only passes constructor args that exist in this pymodbus build.
    """
    kwargs: Dict[str, Any] = {
        "port": str(SERIAL_PORT),
        "baudrate": int(BAUDRATE),
        "bytesize": int(BYTESIZE),
        "stopbits": int(STOPBITS),
        "parity": str(PARITY),
        "timeout": float(TIMEOUT),
    }

    # Some builds accept method=..., some don't.
    try:
        sig = inspect.signature(ModbusSerialClient.__init__)
        if "method" in sig.parameters:
            kwargs["method"] = METHOD
    except Exception:
        pass

    kwargs.update(_rtu_framer_kw())
    return ModbusSerialClient(**kwargs)


@contextmanager
def pzem_client() -> Iterator[ModbusSerialClient]:
    client = make_modbus_client()
    try:
        if not client.connect():
            raise RuntimeError(f"Could not open {SERIAL_PORT}")
        yield client
    finally:
        try:
            client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Public API: Reading
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class PzemReading:
    unit_id: int
    voltage: float
    current: float
    power: float
    energy_wh: float
    raw_hv: int
    raw_lv: int
    raw_regs: list[int]


def read_pzem(
    client: ModbusSerialClient,
    unit_id: int,
    label: Optional[str] = None,
    *,
    verbose: bool = VERBOSE_READ_DEFAULT,
    log_errors: bool = False,
) -> Optional[PzemReading]:
    """
    Read 8 input registers from PZEM-017.
    Returns None on no response / protocol error (caller can treat as <~7V silent).
    """
    _require_unit_id(unit_id)
    ser = _get_serial_port_from_client(client)

    try:
        regs = _rtu_read_registers(
            ser,
            unit_id=unit_id,
            function_code=0x04,
            start_addr=0x0000,
            count=8,
            timeout_s=float(TIMEOUT),
        )
    except PzemModbusError as e:
        if log_errors:
            name = label or f"unit {unit_id}"
            print(f"[{name}] ERROR reading unit {unit_id}: {e} ({e.detail or ''})")
        return None

    raw_v, raw_i, raw_pL, raw_pH, raw_eL, raw_eH, raw_hv, raw_lv = regs

    voltage = raw_v / 100.0
    current = raw_i / 100.0
    power = ((raw_pH << 16) | raw_pL) / 10.0
    energy_wh = ((raw_eH << 16) | raw_eL) * 1.0

    reading = PzemReading(
        unit_id=int(unit_id),
        voltage=float(voltage),
        current=float(current),
        power=float(power),
        energy_wh=float(energy_wh),
        raw_hv=int(raw_hv),
        raw_lv=int(raw_lv),
        raw_regs=list(regs),
    )

    if verbose:
        name = label or f"PZEM unit {unit_id}"
        print(f"\n=== {name} (unit {unit_id}) ===")
        print(f"Voltage : {reading.voltage:.2f} V")
        print(f"Current : {reading.current:.2f} A")
        print(f"Power   : {reading.power:.1f} W")
        print(f"Energy  : {reading.energy_wh:.0f} Wh")
        print(f"HV alarm raw: 0x{reading.raw_hv:04X}")
        print(f"LV alarm raw: 0x{reading.raw_lv:04X}")

    return reading


# ---------------------------------------------------------------------
# Public API: Holding-register params + writes
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class PzemParams:
    high_v_threshold_v: float
    low_v_threshold_v: float
    unit_address: int
    shunt_code: int


def read_params(client: ModbusSerialClient, unit_id: int) -> PzemParams:
    _require_unit_id(unit_id)
    ser = _get_serial_port_from_client(client)

    regs = _rtu_read_registers(
        ser,
        unit_id=unit_id,
        function_code=0x03,
        start_addr=0x0000,
        count=4,
        timeout_s=float(TIMEOUT),
    )

    hv_raw, lv_raw, addr, shunt_code = regs
    return PzemParams(
        high_v_threshold_v=float(hv_raw) / 100.0,
        low_v_threshold_v=float(lv_raw) / 100.0,
        unit_address=int(addr),
        shunt_code=int(shunt_code),
    )


def set_high_voltage_threshold(client: ModbusSerialClient, unit_id: int, volts: float) -> None:
    _require_unit_id(unit_id)
    if not (5.0 <= volts <= 350.0):
        raise ValueError("High voltage threshold must be between 5 and 350 V.")
    value = int(round(volts * 100))
    ser = _get_serial_port_from_client(client)
    _rtu_write_single_register(ser, unit_id=unit_id, addr=0x0000, value=value, timeout_s=float(TIMEOUT))


def set_low_voltage_threshold(client: ModbusSerialClient, unit_id: int, volts: float) -> None:
    _require_unit_id(unit_id)
    if not (1.0 <= volts <= 350.0):
        raise ValueError("Low voltage threshold must be between 1 and 350 V.")
    value = int(round(volts * 100))
    ser = _get_serial_port_from_client(client)
    _rtu_write_single_register(ser, unit_id=unit_id, addr=0x0001, value=value, timeout_s=float(TIMEOUT))


def set_unit_address(client: ModbusSerialClient, current_unit_id: int, new_unit_id: int) -> None:
    _require_unit_id(current_unit_id)
    _require_unit_id(new_unit_id)
    ser = _get_serial_port_from_client(client)
    _rtu_write_single_register(ser, unit_id=current_unit_id, addr=0x0002, value=int(new_unit_id), timeout_s=float(TIMEOUT))


def set_shunt_code(client: ModbusSerialClient, unit_id: int, shunt_code: int) -> None:
    _require_unit_id(unit_id)
    if shunt_code not in (0, 1, 2, 3):
        raise ValueError("shunt_code must be one of: 0, 1, 2, 3")
    ser = _get_serial_port_from_client(client)
    _rtu_write_single_register(ser, unit_id=unit_id, addr=0x0003, value=int(shunt_code), timeout_s=float(TIMEOUT))


# ---------------------------------------------------------------------
# PZEM-specific raw commands (reset energy, calibration)
# ---------------------------------------------------------------------

def reset_energy(client: ModbusSerialClient, unit_id: int) -> None:
    """
    Reset energy command:
      TX: [unit][0x42][CRClo][CRChi]  (4 bytes)
      OK: echo same 4 bytes
      ERR: [unit][0xC2][abnormal][CRClo][CRChi] (5 bytes)
    """
    _require_unit_id(unit_id)
    ser = _get_serial_port_from_client(client)

    req = append_crc(bytes([int(unit_id), 0x42]))
    _best_effort_flush(ser)
    ser.write(req)
    if hasattr(ser, "flush"):
        ser.flush()

    hdr = _read_exact(ser, 2, float(TIMEOUT))
    if len(hdr) < 2:
        raise PzemRawCommandError("Reset energy: no response", unit_id=unit_id)

    if hdr[0] != int(unit_id):
        raise PzemRawCommandError(f"Reset energy: unexpected responder 0x{hdr[0]:02X}", unit_id=unit_id)

    if hdr[1] == 0x42:
        tail = _read_exact(ser, 2, float(TIMEOUT))
        frame = hdr + tail
        if len(frame) != 4 or not _check_crc(frame):
            raise PzemRawCommandError("Reset energy: bad CRC / truncated OK reply", unit_id=unit_id)
        if frame != req:
            raise PzemRawCommandError(f"Reset energy: unexpected OK reply {frame.hex(' ')}", unit_id=unit_id)
        return

    if hdr[1] == 0xC2:
        rest = _read_exact(ser, 3, float(TIMEOUT))
        frame = hdr + rest
        if len(frame) != 5 or not _check_crc(frame):
            raise PzemRawCommandError("Reset energy: bad CRC / truncated error reply", unit_id=unit_id)
        abnormal = frame[2]
        msg = ABNORMAL_CODES.get(abnormal, f"Unknown abnormal code 0x{abnormal:02X}")
        raise PzemRawCommandError(
            f"Reset energy failed: 0x{abnormal:02X} ({msg})",
            unit_id=unit_id,
            abnormal_code=int(abnormal),
        )

    raise PzemRawCommandError(f"Reset energy: unexpected function byte 0x{hdr[1]:02X}", unit_id=unit_id)


def calibrate(client: ModbusSerialClient, *, response_timeout_s: float = 6.0) -> None:
    """
    Calibration command (per your spec):
      TX: [F8][41][37][21][CRClo][CRChi] (6 bytes)
      OK: echo same 6 bytes
      ERR: [slave][C1][abnormal][CRClo][CRChi] (5 bytes)
    """
    ser = _get_serial_port_from_client(client)

    req = append_crc(bytes([0xF8, 0x41, 0x37, 0x21]))

    old_timeout = getattr(ser, "timeout", None)
    try:
        try:
            ser.timeout = max(float(response_timeout_s), float(old_timeout or 0))
        except Exception:
            pass

        _best_effort_flush(ser)
        ser.write(req)
        if hasattr(ser, "flush"):
            ser.flush()

        hdr = _read_exact(ser, 2, float(response_timeout_s))
        if len(hdr) < 2:
            raise PzemRawCommandError("Calibration: no response")

        # OK echo begins with F8 41
        if hdr[0] == 0xF8 and hdr[1] == 0x41:
            rest = _read_exact(ser, 4, float(response_timeout_s))
            frame = hdr + rest
            if len(frame) != 6 or not _check_crc(frame):
                raise PzemRawCommandError("Calibration: bad CRC / truncated OK reply")
            if frame != req:
                raise PzemRawCommandError(f"Calibration: unexpected reply {frame.hex(' ')}")
            return

        # Error frame: [slave][C1][abnormal][CRClo][CRChi]
        if hdr[1] == 0xC1:
            rest = _read_exact(ser, 3, float(response_timeout_s))
            frame = hdr + rest
            if len(frame) != 5 or not _check_crc(frame):
                raise PzemRawCommandError("Calibration: bad CRC / truncated error reply", unit_id=int(hdr[0]))
            abnormal = frame[2]
            msg = ABNORMAL_CODES.get(abnormal, f"Unknown abnormal code 0x{abnormal:02X}")
            raise PzemRawCommandError(
                f"Calibration failed: 0x{abnormal:02X} ({msg})",
                unit_id=int(hdr[0]),
                abnormal_code=int(abnormal),
            )

        raise PzemRawCommandError(f"Calibration: unexpected header {hdr.hex(' ')}")

    finally:
        if old_timeout is not None:
            try:
                ser.timeout = old_timeout
            except Exception:
                pass
