import asyncio

# Kurigram's synchronous compatibility layer expects an event loop to exist
# while plugin modules register their handlers on newer Python versions.
asyncio.set_event_loop(asyncio.new_event_loop())

from plugins.filter import (  # noqa: E402
    _apply_result_filters,
    _build_movie_result_buttons,
    _display_title,
    _variant_label,
    clean_query,
    extract_attributes,
)


def test_display_title_removes_release_name_noise():
    filename = "Aavesham_2024_Malayalam_1080p_AMZN_WEB-DL_DDP5.1_HEVC-Telly.mkv"
    assert _display_title(filename) == ("Aavesham", "2024")
    assert extract_attributes(filename) == ("Malayalam", "1080p")


def test_display_title_cleans_series_and_promotional_handle():
    filename = "Reacher_S02E03_1080p_WEB-DL_English_x265_@channel.mkv"
    assert _display_title(filename) == ("Reacher S02E03", "")


def test_spaced_acronym_and_duplicate_release_title_are_collapsed():
    filename = "K G F Chapter 1 2018 KGF Tamil 720p x265 mkv"
    assert _display_title(filename) == ("KGF Chapter 1", "2018")


def test_variant_button_is_concise_and_has_no_filename_separators():
    file_doc = {
        "file_name": "Aavesham_2024_Malayalam_1080p_WEB-DL.mkv",
        "file_size": 2 * 1024 * 1024 * 1024,
    }
    label = _variant_label(file_doc, show_title=False)
    assert label == "1080p • Malayalam • 2.00 GB"
    assert "_" not in label


def test_search_query_normalizes_filename_separators():
    assert clean_query("Aavesham_2024-Malayalam") == "aavesham 2024 malayalam"


def test_result_filters_can_combine_language_and_quality():
    results = [
        {"file_name": "Movie Malayalam 1080p mkv"},
        {"file_name": "Movie Malayalam 720p mkv"},
        {"file_name": "Movie Tamil 1080p mkv"},
    ]
    data = {"filter_language": "Malayalam", "filter_quality": "1080p"}
    assert _apply_result_filters(results, data) == results[:1]


def test_same_title_buttons_do_not_repeat_the_movie_name():
    results = [
        {"_id": "a", "file_name": "KGF Chapter 1 2018 Tamil 720p mkv", "file_size": 1},
        {"_id": "b", "file_name": "KGF Chapter 1 Tamil HDRip mkv", "file_size": 2},
    ]
    rows, _, _ = _build_movie_result_buttons(results, "session", 0)
    assert all("KGF" not in row[0].text for row in rows)


def test_different_release_years_remain_distinguishable():
    results = [
        {"_id": "a", "file_name": "Aavesham 2024 Malayalam 1080p mkv", "file_size": 1},
        {"_id": "b", "file_name": "Aavesham 1979 Malayalam 720p mp4", "file_size": 2},
    ]
    rows, _, _ = _build_movie_result_buttons(results, "session", 0)
    assert "(2024)" in rows[0][0].text
    assert "(1979)" in rows[1][0].text
