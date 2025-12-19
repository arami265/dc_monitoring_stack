from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import time

import requests

from config.influx import InfluxSettings


class InfluxAuthError(RuntimeError):
    pass


class InfluxHttpError(RuntimeError):
    pass


class InfluxConfigError(RuntimeError):
    pass


def _require_url(url: str) -> str:
    """
    Make sure the URL looks like a real HTTP URL.
    This prevents errors like: "No connection adapters were found for 'localhost:8086/...'"
    """
    if not url:
        raise InfluxConfigError("Influx host_url/base_url is empty.")
    u = url.strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        raise InfluxConfigError(
            f"Influx host_url/base_url must include scheme (http:// or https://). Got: {u!r}"
        )
    return u.rstrip("/")


def _auth_headers(token: str) -> dict[str, str]:
    """
    In your setup, Bearer is required for endpoints (including /health).
    """
    if not token:
        raise InfluxAuthError(
            "Influx token is empty. Expected env var INFLUXDB3_AUTH_TOKEN (or your configured token_env_var)."
        )
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _lp_escape_tag(s: str) -> str:
    # Line protocol tag escaping: commas, spaces, equals
    return (
        s.replace("\\", "\\\\")
         .replace(" ", "\\ ")
         .replace(",", "\\,")
         .replace("=", "\\=")
    )


def _lp_escape_measurement(s: str) -> str:
    return s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


@dataclass(frozen=True)
class InfluxWriteResult:
    status_code: int
    text: str


class InfluxClient:
    """
    Minimal HTTP client for InfluxDB 3 Core (v3 endpoints):
      - /health (GET)
      - /api/v3/write_lp (POST)
      - /api/v3/query_sql (POST)
    """
    def __init__(self, settings: InfluxSettings):
        self.settings = settings
        self.base_url = _require_url(getattr(settings, "host_url", "") or getattr(settings, "base_url", ""))
        self._session = requests.Session()

    def _timeout(self) -> float:
        try:
            return float(self.settings.timeout_s)
        except Exception:
            return 5.0

    def health(self) -> dict[str, Any]:
        url = f"{self.base_url}/health"
        r = self._session.get(
            url,
            headers=_auth_headers(self.settings.token),
            timeout=self._timeout(),
        )
        if r.status_code == 401:
            raise InfluxAuthError("401 Unauthorized on /health. Check Bearer token.")
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    def write_lp(
        self,
        lines: str,
        *,
        database: Optional[str] = None,
        precision: Optional[str] = None,
        accept_partial: bool = True,
        no_sync: bool = False,
    ) -> InfluxWriteResult:
        db = database or self.settings.database
        prec = precision or self.settings.precision
        if not db:
            raise InfluxConfigError("Influx database/db is empty.")
        if not prec:
            prec = "ns"

        url = f"{self.base_url}/api/v3/write_lp"
        params = {
            "db": db,
            "precision": prec,
            "accept_partial": "true" if accept_partial else "false",
            "no_sync": "true" if no_sync else "false",
        }

        r = self._session.post(
            url,
            params=params,
            data=lines,
            headers={**_auth_headers(self.settings.token), "Content-Type": "text/plain; charset=utf-8"},
            timeout=self._timeout(),
        )

        if r.status_code == 401:
            raise InfluxAuthError("401 Unauthorized on write_lp. Check Bearer token.")
        if not (200 <= r.status_code < 300):
            raise InfluxHttpError(f"write_lp failed: HTTP {r.status_code}: {r.text}")

        return InfluxWriteResult(status_code=r.status_code, text=r.text)

    def query_sql(self, query: str, *, database: Optional[str] = None) -> dict[str, Any]:
        db = database or self.settings.database
        if not db:
            raise InfluxConfigError("Influx database/db is empty.")
        if not query.strip():
            raise ValueError("query_sql: query is empty")

        url = f"{self.base_url}/api/v3/query_sql"
        payload = {"db": db, "q": query}

        r = self._session.post(
            url,
            json=payload,
            headers={**_auth_headers(self.settings.token), "Content-Type": "application/json"},
            timeout=self._timeout(),
        )

        if r.status_code == 401:
            raise InfluxAuthError("401 Unauthorized on query_sql. Check Bearer token.")
        if not (200 <= r.status_code < 300):
            raise InfluxHttpError(f"query_sql failed: HTTP {r.status_code}: {r.text}")

        return r.json()


def pzem_reading_to_lp(
    *,
    measurement: str,
    unit_id: int,
    fields: dict[str, Any],
    tags: Optional[dict[str, str]] = None,
    ts_ns: Optional[int] = None,
) -> str:
    """
    Build one line-protocol row.
    - unit_id becomes a tag (unit=<id>)
    - fields are written as numeric fields (ints get trailing i)
    - ts_ns defaults to now (ns)
    """
    if not measurement:
        raise ValueError("measurement is empty")

    m = _lp_escape_measurement(measurement)

    merged_tags = {"unit": str(unit_id)}
    if tags:
        merged_tags.update(tags)

    tag_str = ",".join(
        f"{_lp_escape_tag(str(k))}={_lp_escape_tag(str(v))}"
        for k, v in merged_tags.items()
        if v is not None
    )

    fparts: list[str] = []
    for k, v in fields.items():
        if v is None:
            continue
        key = _lp_escape_tag(str(k))
        if isinstance(v, bool):
            fparts.append(f"{key}={'true' if v else 'false'}")
        elif isinstance(v, int):
            fparts.append(f"{key}={v}i")
        elif isinstance(v, float):
            fparts.append(f"{key}={v}")
        else:
            s = str(v).replace("\\", "\\\\").replace('"', '\\"')
            fparts.append(f'{key}="{s}"')

    if not fparts:
        raise ValueError("No fields to write (all fields were None/empty).")

    field_str = ",".join(fparts)
    ts = ts_ns if ts_ns is not None else time.time_ns()

    return f"{m},{tag_str} {field_str} {ts}"
