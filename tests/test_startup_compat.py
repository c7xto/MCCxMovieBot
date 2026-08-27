import ast
from pathlib import Path


BOT_SOURCE = Path(__file__).resolve().parents[1] / "bot.py"


def _bot_tree():
    return ast.parse(BOT_SOURCE.read_text(encoding="utf-8"))


def test_start_override_accepts_current_kurigram_runtime_options():
    tree = _bot_tree()
    bot_class = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "AutoFilterBot"
    )
    start = next(
        node for node in bot_class.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "start"
    )
    keyword_names = {argument.arg for argument in start.args.kwonlyargs}

    assert {"use_qr", "except_ids"} <= keyword_names
    assert start.args.kwarg is not None


def test_startup_check_avoids_deprecated_get_event_loop_call():
    tree = _bot_tree()
    calls = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "get_event_loop" not in calls
    assert "get_running_loop" in calls


def test_python_sources_do_not_use_invalid_logging_comma_specifier():
    project_root = BOT_SOURCE.parent
    invalid_specifier = "%" + ",d"
    offenders = [
        path.relative_to(project_root).as_posix()
        for path in project_root.rglob("*.py")
        if not any(part.startswith(".venv") for part in path.parts)
        and invalid_specifier in path.read_text(encoding="utf-8", errors="replace")
    ]
    assert offenders == []
