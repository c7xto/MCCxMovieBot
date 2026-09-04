"""Conservative release identity shared by ingestion, review and posters."""

import re
import unicodedata

from database.releases import digest


def title_key(title):
    return " ".join(re.findall(r"\w+", unicodedata.normalize("NFKC", title).casefold().replace("_", " ")))


def parse_release(filename):
    # Reuse the listing parser instead of maintaining another junk-word list.
    from plugins.filter import _display_title, _PROMO_RE, LANGUAGES, QUALITIES

    name = re.sub(r"[._]", " ", _PROMO_RE.sub(" ", filename))
    marker = re.search(
        r"(?<!\w)(?:s(?:eason)?\s*(\d{1,2})(?:\s*e(?:pisode)?\s*(\d{1,3}))?"
        r"|(\d{1,2})x(\d{1,3})|e(?:pisode)?\s*(\d{1,3}))(?!\d)",
        name,
        re.I,
    )
    title, year = _display_title(name[: marker.start()] if marker else filename)
    if marker and not year:
        _, year = _display_title(filename)
    season = int(marker[1] or marker[3]) if marker and (marker[1] or marker[3]) else None
    episode = (
        int(marker[2] or marker[4] or marker[5]) if marker and (marker[2] or marker[4] or marker[5]) else None
    )
    episodes = [episode] if episode is not None else []
    if marker and episode is not None:
        end = re.match(r"\s*-\s*(?:e(?:pisode)?\s*)?(\d{1,3})\b", name[marker.end() :], re.I)
        if end and episode <= int(end[1]) <= episode + 100:
            episodes = list(range(episode, int(end[1]) + 1))
    languages = [
        lang for lang in LANGUAGES if re.search(r"\b" + lang.replace(" ", r"\s+") + r"\b", name, re.I)
    ]
    qualities = [q for q in QUALITIES if re.search(r"\b" + q.replace(" ", r"\s*") + r"\b", name, re.I)]
    qualities = sorted(set("HD Rip" if q == "HDRip" else q for q in qualities))
    kind = "tv" if marker else "movie"
    return {
        "title": title,
        "year": year,
        "kind": kind,
        "season": season,
        "episodes": episodes,
        "languages": languages,
        "qualities": qualities,
        "identity": digest(f"{kind}:{title_key(title)}:{year}:{season}"),
    }


def choose_match(parsed, results):
    """Only a unique exact normalized title/year match is automatic.

    For television, a filename year can describe a season rather than the
    premiere. It therefore cannot safely select between same-name shows.
    """
    matches = []
    for row in results:
        names = [
            row.get("title", ""),
            row.get("original_title", ""),
            row.get("name", ""),
            row.get("original_name", ""),
        ]
        if title_key(parsed["title"]) not in {title_key(name) for name in names if name}:
            continue
        if parsed["kind"] == "movie" and parsed["year"]:
            if str(row.get("release_date", ""))[:4] != parsed["year"]:
                continue
        matches.append(row)
    return matches[0] if len(matches) == 1 else None


def render_release(post, summary):
    from html import escape

    meta = post["metadata"]
    title = meta["title"][:100]
    if meta.get("year"):
        title += f" ({meta['year']})"
    if post.get("season") is not None:
        title += f" · Season {post['season']}"
    lines = [f"🎬 <b>{escape(title)}</b>"]
    details = list(meta.get("genres", []))[:3]
    if meta.get("rating"):
        details.append(f"TMDB: {meta['rating']:.1f}/10")
    if details:
        lines.append(escape(" · ".join(details)))
    if meta.get("overview"):
        lines.extend(["", escape(meta["overview"][:240].strip())])
    lines.append("")
    if summary["languages"]:
        lines.append("Languages: " + escape(", ".join(summary["languages"])))
    if summary["qualities"]:
        lines.append("Quality: " + escape(", ".join(summary["qualities"])))
    if summary["episodes"]:
        # Compress only contiguous runs, never imply missing episodes exist.
        runs = []
        start = end = summary["episodes"][0]
        for value in summary["episodes"][1:]:
            if value == end + 1:
                end = value
            else:
                runs.append(str(start) if start == end else f"{start}–{end}")
                start = end = value
        runs.append(str(start) if start == end else f"{start}–{end}")
        rendered = ", ".join(runs)
        if len(rendered) > 100:
            rendered = f"{len(summary['episodes'])} episodes (see files)"
        lines.append("Episodes available: " + rendered)
    lines.append(f"Files available: {summary['count']}")
    return "\n".join(lines)
