"""Required MongoDB index policy, independent of application startup."""


class RequiredIndexError(RuntimeError):
    pass


def has_exact_unique_index(indexes: dict, field: str) -> bool:
    expected = [(field, 1)]
    return any(
        spec.get("key") == expected and spec.get("unique") is True
        for spec in indexes.values()
    )


def has_exact_index(indexes: dict, field: str) -> bool:
    expected = [(field, 1)]
    return any(spec.get("key") == expected for spec in indexes.values())


def has_compound_index(indexes: dict, keys: list[tuple[str, int]], *, unique=False) -> bool:
    expected = list(keys)
    return any(
        spec.get("key") == expected and (not unique or spec.get("unique") is True)
        for spec in indexes.values()
    )


async def ensure_required_index(collection, field: str, label: str):
    indexes = await collection.index_information()
    if has_exact_index(indexes, field):
        return
    try:
        await collection.create_index(field)
    except Exception as exc:
        raise RequiredIndexError(
            f"Could not create required index {label}: {exc}. Run "
            "`python tools/migrate_search_tokens.py` and retry startup."
        ) from exc
    verified = await collection.index_information()
    if not has_exact_index(verified, field):
        raise RequiredIndexError(
            f"MongoDB did not report required index {label} after creation."
        )


async def ensure_required_unique_index(collection, field: str, label: str):
    indexes = await collection.index_information()
    if has_exact_unique_index(indexes, field):
        return

    exact_non_unique = [
        name
        for name, spec in indexes.items()
        if spec.get("key") == [(field, 1)] and spec.get("unique") is not True
    ]
    if exact_non_unique:
        raise RequiredIndexError(
            f"Required unique index {label} is blocked by non-unique index(es) "
            f"{exact_non_unique}. Run `python tools/repair_registry_index.py` "
            f"for a dry run, then repeat with `--apply`."
        )

    try:
        await collection.create_index(field, unique=True)
    except Exception as exc:
        raise RequiredIndexError(
            f"Could not create required unique index {label}: {exc}. Run "
            f"`python tools/repair_registry_index.py` for diagnosis."
        ) from exc

    verified = await collection.index_information()
    if not has_exact_unique_index(verified, field):
        raise RequiredIndexError(
            f"MongoDB did not report required index {label} as unique after creation."
        )


async def ensure_required_compound_index(
    collection,
    keys: list[tuple[str, int]],
    label: str,
    *,
    unique: bool = False,
):
    """Create and verify an exact compound index used by a critical route."""
    indexes = await collection.index_information()
    if has_compound_index(indexes, keys, unique=unique):
        return
    try:
        await collection.create_index(keys, unique=unique, name=label.replace(".", "_"))
    except Exception as exc:
        raise RequiredIndexError(f"Could not create required index {label}: {exc}") from exc
    verified = await collection.index_information()
    if not has_compound_index(verified, keys, unique=unique):
        qualifier = " unique" if unique else ""
        raise RequiredIndexError(
            f"MongoDB did not report required{qualifier} compound index {label} after creation."
        )
