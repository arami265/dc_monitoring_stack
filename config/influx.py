from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

try:
    import tomllib  # py3.11+
except ModuleNotFoundError:  # pragma: no cover
    raise RuntimeError("Python 3.11+ required for tomllib. Use a newer Python or install tomli.")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOML_PATH = PROJECT_ROOT / "config.toml"


@dataclass(frozen=True)
class InfluxSettings:
    host_url: str
    database: str
    precision: str = "ns"
    token: str = ""
    timeout_s: float = 5.0

    # Used for measurement naming defaults
    measurement: str = "pzem_dc"
    poll_interval_s: float = 1.0

    # Tags applied to every write
    global_tags: dict[str, str] = None  # type: ignore[assignment]

def _load_env_file_if_present(path: Path) -> None:
    """
    Minimal .env loader:
      - Does NOT override existing environment variables
      - Ignores comments/blank lines
      - Supports KEY=VALUE (optionally quoted)
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        # Do not overwrite real environment
        if key and key not in os.environ:
            os.environ[key] = value


def load_influx_settings(toml_path: Path = DEFAULT_TOML_PATH) -> InfluxSettings:
    raw = tomllib.loads(toml_path.read_text(encoding="utf-8"))

    influx = raw.get("influx", {})
    poller = raw.get("poller", {})
    tags = raw.get("tags", {})

    token_env = influx.get("token_env_var", "INFLUXDB3_AUTH_TOKEN")
    if not os.getenv(token_env):
        _load_env_file_if_present(PROJECT_ROOT / ".env")

    token = os.getenv(token_env) or influx.get("token_fallback", "")

    return InfluxSettings(
        host_url=influx.get("host_url", "http://127.0.0.1:8181"),
        database=influx.get("database", "pzem_demo"),
        precision=influx.get("precision", "ns"),
        token=token,
        timeout_s=float(influx.get("timeout_s", 5.0)),
        measurement=poller.get("measurement", "pzem_dc"),
        poll_interval_s=float(poller.get("interval_s", 1.0)),
        global_tags=dict(tags),
    )
