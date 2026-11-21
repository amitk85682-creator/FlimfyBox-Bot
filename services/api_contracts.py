"""
Standardized API response contract helpers shared across Python routes,
aligned 1-to-1 with Node.js `src/contracts.ts`.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def resolve_data_mode(has_database: bool) -> str:
    configured = (os.environ.get("APP_DATA_MODE") or "").strip().lower()
    if configured in ("production", "demo"):
        return configured
    return "production" if has_database else "demo"


def ok_payload(data: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None) -> Dict[str, Any]:
    body: Dict[str, Any] = {"status": "success"}
    if request_id:
        body["request_id"] = request_id
    if data:
        body.update(data)
    return body


def error_payload(
    code: str,
    message: str,
    request_id: str = "unknown",
) -> Dict[str, Any]:
    return {
        "status": "error",
        "code": code,
        "message": message,
        "request_id": request_id,
    }


def health_payload(
    *,
    service: str,
    db_connected: bool,
    uptime_seconds: float,
    version: str = "1.0.0",
) -> Dict[str, Any]:
    return {
        "status": "ok" if db_connected or resolve_data_mode(db_connected) == "demo" else "degraded",
        "service": service,
        "version": version,
        "data_mode": resolve_data_mode(db_connected),
        "database": {"connected": db_connected},
        "uptime_seconds": round(max(0.0, uptime_seconds), 2),
    }
