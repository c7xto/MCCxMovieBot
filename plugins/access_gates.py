"""Canonical access-gate schema with one-release legacy compatibility."""

from __future__ import annotations

import hashlib


ACCESS_GATES_SCHEMA_VERSION = 1
REQUIRED_INTERVAL_SECONDS = 15 * 60
GRACE_SECONDS = 15 * 60
DEFAULT_TIMED_INTERVAL_SECONDS = 24 * 60 * 60


def _channel_id(entry):
    return entry.get("id") if isinstance(entry, dict) else entry


def _stored_link(entry):
    return entry.get("link") if isinstance(entry, dict) else None


def gate_key(channel_id, mode: str) -> str:
    raw = f"{mode}:{str(channel_id).strip().casefold()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _normalize_gate(entry: dict, index: int = 0) -> dict | None:
    if not isinstance(entry, dict):
        return {
            "key": gate_key(f"invalid-{index}", "required"),
            "id": None,
            "label": "Invalid Access Gate",
            "link": None,
            "mode": "required",
            "interval_seconds": REQUIRED_INTERVAL_SECONDS,
            "enabled": True,
            "source": "invalid",
        }
    channel_id = entry.get("id")
    mode = str(entry.get("mode", "")).casefold()
    if mode not in {"required", "timed"}:
        channel_id = None
        mode = "required"
    interval = entry.get("interval_seconds", REQUIRED_INTERVAL_SECONDS)
    try:
        interval = int(interval)
    except (TypeError, ValueError):
        interval = REQUIRED_INTERVAL_SECONDS
    if mode == "required":
        interval = REQUIRED_INTERVAL_SECONDS
    else:
        interval = max(60, min(interval, 30 * 24 * 60 * 60))
    key = str(entry.get("key") or gate_key(channel_id or f"invalid-{index}", mode))
    return {
        "key": key,
        "id": channel_id,
        "label": str(entry.get("label") or "Access Channel")[:64],
        "link": entry.get("link"),
        "mode": mode,
        "interval_seconds": interval,
        "enabled": bool(entry.get("enabled", True)),
        "source": str(entry.get("source") or "access_gates")[:32],
    }


def legacy_access_gates(config: dict) -> list[dict]:
    """Translate old Main/Request/Two-Stage fields without changing them."""
    candidates = []
    main_entries = config.get("fsub_channels", [])
    if not isinstance(main_entries, list):
        main_entries = [None]
    for index, entry in enumerate(main_entries, 1):
        channel_id = _channel_id(entry)
        candidates.append(
            {
                "key": gate_key(channel_id or f"main-{index}", "required"),
                "id": channel_id,
                "label": f"Required Channel {index}",
                "link": _stored_link(entry),
                "mode": "required",
                "interval_seconds": REQUIRED_INTERVAL_SECONDS,
                "enabled": True,
                "source": "main_fsub",
            }
        )

    try:
        request_interval = int(config.get("req_fsub_interval_hours", 24)) * 3600
    except (TypeError, ValueError):
        request_interval = DEFAULT_TIMED_INTERVAL_SECONDS
    request_interval = max(60, min(request_interval, 30 * 24 * 60 * 60))
    request_entries = config.get("req_fsub_channels", [])
    if not isinstance(request_entries, list):
        request_entries = [None]
    for index, entry in enumerate(request_entries, 1):
        channel_id = _channel_id(entry)
        candidates.append(
            {
                "key": gate_key(channel_id or f"request-{index}", "timed"),
                "id": channel_id,
                "label": f"Timed Channel {index}",
                "link": _stored_link(entry),
                "mode": "timed",
                "interval_seconds": request_interval,
                "enabled": True,
                "source": "request_fsub",
            }
        )

    two_stage_entries = config.get("two_stage_channels", [])
    if not isinstance(two_stage_entries, list):
        two_stage_entries = [None]
    active_two_stage = [entry for entry in two_stage_entries if entry]
    if len(active_two_stage) >= 2:
        for index, entry in enumerate(active_two_stage, 1):
            channel_id = _channel_id(entry)
            candidates.append(
                {
                    "key": gate_key(channel_id or f"two-stage-{index}", "timed"),
                    "id": channel_id,
                    "label": f"Timed Channel {index}",
                    "link": _stored_link(entry),
                    "mode": "timed",
                    "interval_seconds": 30 * 60,
                    "enabled": True,
                    "source": "two_stage",
                }
            )

    # Required is stricter than timed. If the same channel exists in both old
    # systems, keep only the required gate so users never see duplicate joins.
    by_channel = {}
    invalid = []
    for gate in candidates:
        identity = str(gate.get("id") or "").strip().casefold()
        if not identity:
            invalid.append(gate)
            continue
        current = by_channel.get(identity)
        if current is None or gate["mode"] == "required":
            by_channel[identity] = gate
    return [*by_channel.values(), *invalid]


def get_access_gates(config: dict) -> list[dict]:
    """Read canonical gates, falling back to old fields for one release."""
    raw = config.get("access_gates")
    try:
        schema_version = int(config.get("access_gates_schema_version", 0) or 0)
    except (TypeError, ValueError):
        schema_version = 0
    if isinstance(raw, list) and schema_version >= 1:
        return [gate for i, item in enumerate(raw) if (gate := _normalize_gate(item, i))["enabled"]]
    return legacy_access_gates(config)


def access_gate_health(gate: dict) -> tuple[bool, str]:
    if not gate.get("id"):
        return False, "missing channel ID"
    if gate.get("mode") not in {"required", "timed"}:
        return False, "invalid mode"
    if not gate.get("link") and str(gate.get("id", "")).lstrip("@").lstrip("-").isdigit():
        return False, "private channel has no stored invite link"
    return True, "configured"
