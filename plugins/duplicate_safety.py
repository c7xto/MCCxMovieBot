"""Pure helpers for safe, idempotent duplicate-group deletion."""

import hashlib
import re


_OBJECT_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")


def stable_duplicate_key(kind: str, identity: str) -> str:
    prefix = "e" if kind == "exact" else "f"
    digest = hashlib.sha256(f"{kind}:{identity}".encode("utf-8")).hexdigest()[:16]
    return prefix + digest


def select_keep_and_delete(object_ids) -> tuple[str, list[str]]:
    """Keep the lexically oldest ObjectId and return all unique extras."""
    unique_ids = list(dict.fromkeys(str(value) for value in object_ids))
    if len(unique_ids) < 2:
        raise ValueError("A duplicate group must contain at least two records")
    if any(not _OBJECT_ID_RE.fullmatch(value) for value in unique_ids):
        raise ValueError("Duplicate group contains an invalid ObjectId")
    keep_id = min(unique_ids)
    return keep_id, [value for value in unique_ids if value != keep_id]
