from plugins.index_progress import IndexProgress, progress_bar, readable_duration


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value


def test_progress_uses_resume_range_and_rolling_eta():
    clock = Clock()
    progress = IndexProgress(101, 200, now=clock)
    clock.value = 5
    progress.record_batch(
        end_id=150, media=30, saved=20, duplicates=10, skipped=20
    )

    assert progress.total == 100
    assert progress.scanned == 50
    assert progress.percentage == 50
    assert progress.rolling_speed == 10
    assert progress.eta == 5
    assert "50 / 100" in progress.render()
    assert "Saved through message `150`" in progress.render()


def test_progress_ui_is_bounded_and_plain():
    assert progress_bar(-1) == "▱" * 10
    assert progress_bar(1000) == "▰" * 10
    assert readable_duration(None) == "Calculating…"
    assert readable_duration(3661) == "1h 1m"

