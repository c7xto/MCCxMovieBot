import asyncio
from types import SimpleNamespace

# Kurigram's synchronous compatibility layer expects an event loop to exist
# while plugin modules register their handlers on newer Python versions.
asyncio.set_event_loop(asyncio.new_event_loop())

from plugins.filter import (  # noqa: E402
    _apply_result_filters,
    _build_movie_result_buttons,
    _build_results_caption,
    _display_title,
    _flat_file_label,
    _listing_name,
    _sort_results,
    _variant_label,
    clean_query,
    extract_attributes,
)
from plugins.search_indicator import _select_search_sticker  # noqa: E402
from plugins.start import _build_start_ui  # noqa: E402


def test_search_indicator_selects_native_animated_magnifying_sticker():
    stickers = [
        SimpleNamespace(emoji="🔎", is_animated=False, file_id="static"),
        SimpleNamespace(emoji="🔍", is_animated=True, file_id="animated"),
    ]
    assert _select_search_sticker(stickers).file_id == "animated"


def test_home_screen_uses_live_count_and_clean_button_order():
    text, markup = _build_start_ui(
        {}, "Tester", 1_130_509, "MCCxMovieBot", "https://t.me/releases",
        "https://t.me/requests", first_name="Tester",
    )

    assert "1,130,509 files available" in text
    assert "Aavesham" not in text
    assert [[button.text for button in row] for row in markup.inline_keyboard] == [
        ["🔎 Search Guide", "🌐 മലയാളം"],
        ["📝 Request Movie", "📢 New Releases"],
        ["➕ Add Bot to Group"],
    ]


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


def test_series_label_keeps_only_series_identity_and_structured_metadata():
    file_doc = {
        "_id": "a",
        "file_name": "Reacher_S01E03_Spoonful_2022_1080p-H265_DDP5.1_@spam.mkv",
        "file_size": 2 * 1024 * 1024 * 1024,
    }
    label = _flat_file_label(file_doc)
    assert label == "[2.00 GB] [S01E03] Reacher • 1080p • HEVC"
    assert "Spoonful" not in label
    assert not any(char in label for char in "_-@")
    assert "mkv" not in label.lower()


def test_movie_label_uses_clean_title_year_and_fixed_metadata_order():
    file_doc = {
        "file_name": "Aavesham_2024_Malayalam_1080p_WEB-DL_x265.mkv",
        "file_size": 500 * 1024 * 1024,
    }
    assert _flat_file_label(file_doc) == (
        "[500.00 MB] Aavesham (2024) • Malayalam • 1080p • HEVC"
    )


def test_long_series_title_is_trimmed_without_hiding_quality_fields():
    file_doc = {
        "file_name": (
            "A Very Long Series Name That Must Be Shortened S02E04 "
            "An Even Longer Episode Name English 1080p x265.mkv"
        ),
        "file_size": 700 * 1024 * 1024,
    }
    label = _flat_file_label(file_doc)
    assert len(label) <= 64
    assert label.startswith("[700.00 MB] [S02E04]")
    assert "An Even Longer Episode Name" not in label
    assert label.endswith("English • 1080p • HEVC")


def test_listing_name_removes_brackets_extension_and_promotional_url():
    name, episode = _listing_name(
        "[Reacher].S2E8.English+WEB-DL.#x265| https://t.me/spam mkv"
    )
    assert name == "Reacher English WEB DL x265"
    assert episode == "S02E08"


def test_flat_results_show_ten_files_without_grouping():
    results = [
        {
            "_id": str(index),
            "file_name": f"KGF Chapter 2 2022 Tamil {index} 1080p mkv",
            "file_size": index * 1024 * 1024,
        }
        for index in range(1, 12)
    ]
    rows, page, total_pages = _build_movie_result_buttons(results, "session", 0)
    assert page == 0
    assert total_pages == 2
    assert len(rows) == 11  # ten files plus NEXT
    assert all("KGF Chapter 2" in row[0].text for row in rows[:10])
    assert rows[-1][0].text == "NEXT ➡"


def test_results_caption_contains_shared_count_and_page_header():
    caption = _build_results_caption("reacher 2022", 152, 0, 16, "7")
    assert "Results Found For reacher 2022" in caption
    assert "Files:</b> 152" in caption
    assert "Page:</b> 1 / 16" in caption
    assert "👤 <b>7</b>" in caption


def test_movie_results_sort_from_smallest_to_largest():
    results = [
        {"_id": "large", "file_name": "Movie 1080p", "file_size": 999},
        {"_id": "small", "file_name": "Movie 480p", "file_size": 1},
        {"_id": "medium", "file_name": "Movie 720p", "file_size": 500},
    ]
    assert [item["_id"] for item in _sort_results(results)] == ["small", "medium", "large"]


def test_series_results_sort_by_season_then_episode():
    results = [
        {"_id": "movie", "file_name": "Reacher Movie 2012", "file_size": 1},
        {"_id": "s2e1", "file_name": "Reacher S02E01 1080p", "file_size": 300},
        {"_id": "s1e8", "file_name": "Reacher Season 1 Episode 8", "file_size": 200},
        {"_id": "s1e2-large", "file_name": "Reacher S01 EP02 1080p", "file_size": 400},
        {"_id": "s1e2-small", "file_name": "Reacher S01E02 720p", "file_size": 100},
        {"_id": "e3", "file_name": "Reacher E03 480p", "file_size": 50},
    ]
    assert [item["_id"] for item in _sort_results(results)] == [
        "s1e2-small", "s1e2-large", "e3", "s1e8", "s2e1", "movie"
    ]


def test_smart_result_deduplication_hides_duplicate_episode_titles():
    results = [
        {
            "_id": "first",
            "file_name": "Reacher S01E01 Welcome to Margrave 720p x265.mkv",
            "file_size": 300,
        },
        {
            "_id": "duplicate",
            "file_name": "Reacher S01E01 Pilot Episode 720p x265.mp4",
            "file_size": 300,
        },
        {
            "_id": "size-variant",
            "file_name": "Reacher S01E01 720p x265.mkv",
            "file_size": 400,
        },
    ]

    assert [item["_id"] for item in _sort_results(results)] == [
        "first", "size-variant"
    ]
