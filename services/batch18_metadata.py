"""
Smart Adult / OTT Filename Parser & Multi-Source Metadata Engine for /batch18.

Designed to accurately parse and resolve ANY adult movie, erotic film,
international 18+ release, or Indian OTT web series (Ullu, ALTBalaji/ALTT,
Atrangii, PrimeShots, Kooku, Voovi, MoodX, Hunters, Rabbit, Woow, NeonX,
Fliz, Nuefliks, Hollywood, Korean, European, Japanese, etc.).

All parsing, reconciliation, query-variant generation, and candidate scoring
functions here are pure and unit-tested.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote


# ---------------------------------------------------------------------------
# Platform, Studio & Junk Vocabularies
# ---------------------------------------------------------------------------

INDIAN_OTT_PLATFORMS: Tuple[Tuple[str, str], ...] = (
    (r"\bullu\s*originals?\b|\bullu\b", "Ullu"),
    (r"\balt\s*balaji\b|\baltt\b", "ALTBalaji"),
    (r"\batrangii\s*originals?\b|\batrangii\b", "Atrangii"),
    (r"\bprime\s*shots?\b|\bprimeshots\b", "PrimeShots"),
    (r"\bkooku\s*originals?\b|\bkooku\b", "Kooku"),
    (r"\bvoovi\b", "Voovi"),
    (r"\bmoodx\b|\bmood\s*x\b", "MoodX"),
    (r"\bhunters?\s*originals?\b|\bhunters\b", "Hunters"),
    (r"\brabbit\s*movies?\b|\brabbit\b", "Rabbit"),
    (r"\bbesharams?\b", "Besharams"),
    (r"\bhit\s*prime\b|\bhitprime\b", "HitPrime"),
    (r"\bhot\s*hit\b|\bhothit\b", "HotHit"),
    (r"\bfliz\s*movies?\b|\bfliz\b", "Fliz"),
    (r"\bnuefliks\b", "Nuefliks"),
    (r"\bbig\s*movie\s*zoo\b|\bbigmoviezoo\b", "BigMovieZoo"),
    (r"\bwoow\b", "Woow"),
    (r"\bneonx\b|\bneon\s*x\b", "NeonX"),
    (r"\bhotx\b|\bhot\s*x\b", "HotX"),
    (r"\bakku\s*ott\b|\bakkuott\b", "AkkuOTT"),
    (r"\bcineprime\b|\bcine\s*prime\b", "CinePrime"),
    (r"\bcheeku\s*flix\b|\bcheekuflix\b", "CheekuFlix"),
    (r"\bboom\s*movies?\b|\bboommovies\b", "BoomMovies"),
    (r"\bdigimovieplex\b", "DigiMoviePlex"),
    (r"\bmx\s*originals?\b|\bmx\s*player\b", "MX Player"),
    (r"\bhoichoi\b", "Hoichoi"),
    (r"\bshemaroo\s*me\b|\bshemaroo\b", "ShemarooMe"),
    (r"\baddatimes\b", "Addatimes"),
    (r"\bklikk\b", "KliKK"),
    (r"\bchaupal\b", "Chaupal"),
)

GLOBAL_ADULT_STUDIOS: Tuple[Tuple[str, str], ...] = (
    (r"\bbrazzers\b", "Brazzers"),
    (r"\bvixen\b", "Vixen"),
    (r"\bblacked(?:\s*raw)?\b", "Blacked"),
    (r"\btushy(?:\s*raw)?\b", "Tushy"),
    (r"\bdeeper\b", "Deeper"),
    (r"\bevil\s*angel\b", "Evil Angel"),
    (r"\bmarc\s*dorcel\b|\bdorcel\b", "Dorcel"),
    (r"\bdigital\s*playground\b", "Digital Playground"),
    (r"\bplayboy\b", "Playboy"),
    (r"\bpenthouse\b", "Penthouse"),
    (r"\bprivate\s*gold\b", "Private"),
    (r"\bvivid\b", "Vivid"),
)

GENERIC_CHANNEL_TITLES = frozenset({
    "",
    "unknown",
    "unrated",
    "unrated series",
    "unrated series hd",
    "unrated movies",
    "unrated web series",
    "adult",
    "adult movies",
    "adult web series",
    "adult series",
    "adult hub",
    "adult zone",
    "18",
    "18 plus",
    "18 movies",
    "18 web series",
    "hot",
    "hot movies",
    "hot web series",
    "hot series",
    "ullu",
    "ullu originals",
    "ullu web series",
    "ullu latest",
    "primeshots",
    "altbalaji",
    "atrangii",
    "kooku",
    "voovi",
    "web series",
    "hindi web series",
    "full movie",
    "full episode",
    "video",
    "movie",
    "series",
    "file",
    "download",
    "telegram",
    "channel",
    "backup",
    "latest",
    "new",
    "hd",
    "uncut",
    "uncensored",
    "desi",
    "desi hot",
})

LANGUAGE_PATTERNS: Tuple[Tuple[str, str], ...] = (
    (r"\bhindi\b|हिन्दी|हिंदी", "Hindi"),
    (r"\benglish\b|\beng\b", "English"),
    (r"\btamil\b|தமிழ்", "Tamil"),
    (r"\btelugu\b|తెలుగు", "Telugu"),
    (r"\bmalayalam\b", "Malayalam"),
    (r"\bkannada\b", "Kannada"),
    (r"\bbengali\b|\bbangla\b", "Bengali"),
    (r"\bmarathi\b", "Marathi"),
    (r"\bpunjabi\b", "Punjabi"),
    (r"\bgujarati\b", "Gujarati"),
    (r"\bbhojpuri\b", "Bhojpuri"),
    (r"\bkorean\b|\bkor\b", "Korean"),
    (r"\bjapanese\b|\bjpn\b|\bjav\b|日本語", "Japanese"),
    (r"\bchinese\b|\bmandarin\b|\bcantonese\b", "Chinese"),
    (r"\bfrench\b", "French"),
    (r"\bspanish\b", "Spanish"),
    (r"\bitalian\b", "Italian"),
    (r"\bgerman\b", "German"),
    (r"\brussian\b", "Russian"),
    (r"\bthai\b", "Thai"),
    (r"\btagalog\b|\bfilipino\b|\bvivamax\b", "Filipino"),
    (r"\bdual[\s._-]*audio\b", "Dual Audio"),
    (r"\bmulti[\s._-]*audio\b", "Multi Audio"),
)

# Common Hindi / Hinglish spelling alternates for IMDb / TMDB query expansion
HINGLISH_SPELLING_SWAPS: Tuple[Tuple[str, str], ...] = (
    (r"\bpyar\b", "pyaar"),
    (r"\bpyaar\b", "pyar"),
    (r"\bbhabhi\b", "bhabi"),
    (r"\bbhabi\b", "bhabhi"),
    (r"\bishq\b", "ishk"),
    (r"\braat\b", "rat"),
    (r"\bkhwab\b", "khwaab"),
    (r"\bkhwaab\b", "khwab"),
    (r"\bgandii\b", "gandi"),
    (r"\bgandi\b", "gandii"),
    (r"\bchawl\b", "chaul"),
    (r"\bmastani\b", "mastaani"),
    (r"\bpalangtod\b", "palang tod"),
    (r"\bpalang\s+tod\b", "palangtod"),
)

JAV_CODE_RE = re.compile(
    r"\b((?:FC2[\s_-]*PPV[\s_-]*\d{5,8})|(?:[A-Z]{2,6}[-_]\d{3,5}))\b",
    re.I,
)

VIDEO_EXT_RE = re.compile(r"\.(?:mkv|mp4|avi|mov|m4v|webm|ts|wmv|flv)$", re.I)

# Technical / release / promo token regex used to split & strip noise from titles
TECH_SPLIT_RE = re.compile(
    r"(?:"
    r"\b(?:19|20)\d{2}\b|"
    r"\bS(?:EASON)?[\s._-]*0*\d{1,2}(?:[\s._-]*E(?:P(?:ISODE)?)?[\s._-]*0*\d{1,3}(?:\s*(?:[-~_]|to)\s*(?:E|EP)?0*\d{1,3})?)?\b|"
    r"\bS(?:EASON)?[\s._-]*0*\d{1,2}[\s._-]*P(?:ART)?[\s._-]*0*\d{1,3}\b|"
    r"\b(?:EP(?:ISODE)?|E)[\s._-]*0*\d{1,3}(?:\s*(?:[-~_]|to)\s*(?:E|EP)?0*\d{1,3})?\b|"
    r"\bPART[\s._-]*0*\d{1,3}\b|"
    r"\bVOL(?:UME)?\.?[\s._-]*(?:[IVX]{1,4}|\d{1,2})\b|"
    r"\b(?:2160|1440|1080|720|576|480|360)p\b|\b4K\b|\bUHD\b|"
    r"\b(?:WEB[\s._-]?DL|WEBRip|HDRip|Blu[\s._-]?Ray|BRRip|BDRip|DVDRip|HDTV|HDTC|HDTS|HDCAM|CAMRip|PreDVD|DVDScr|Remux|DS4K)\b|"
    r"\b(?:x264|x265|H[\s.]?264|H[\s.]?265|HEVC|AVC|10bit|8bit|DDP?\s*\d(?:\.\d)?|AAC\s*\d?(?:\.\d)?|AC3|DTS|Atmos|TrueHD|ESub|MSub|HC[\s._-]?Sub|Subbed|Dubbed)\b|"
    r"\b(?:UNRATED|UNCUT|UNCENSORED|EXTENDED|DIRECTOR'?S?\s*CUT|REMASTERED|THEATRICAL|COMBINED|COMPLETE|FULL\s*(?:MOVIE|SERIES|SEASON|EPISODE)|ALL\s*EPISODES)\b|"
    r"\b(?:WEB[\s._-]*SERIES|WEB[\s._-]*SE\b|ADULT\s*MOVIE|ADULT\s*FILM|SHORT\s*FILM|HOT\s*MOVIE|18\+|XXX|EROTIC|SOFTCORE|HARDCORE)\b|"
    r"\b(?:HINDI|ENGLISH|TAMIL|TELUGU|MALAYALAM|KANNADA|BENGALI|MARATHI|PUNJABI|KOREAN|JAPANESE|FRENCH|SPANISH|ITALIAN|RUSSIAN|THAI|FILIPINO|TAGALOG|DUAL[\s._-]*AUDIO|MULTI[\s._-]*AUDIO)\b|"
    r"\b(?:ULLU|ALTBALAJI|ALTT|ATRANGII|PRIMESHOTS|PRIME\s*SHOTS|KOOKU|VOOVI|MOODX|HUNTERS|RABBIT|BESHARAMS|HITPRIME|HOTHIT|FLIZ|NUEFLIKS|BIGMOVIEZOO|WOOW|NEONX|HOTX|AKKUOTT|CINEPRIME|CHEEKUFLIX|BOOMMOVIES|DIGIMOVIEPLEX|VIVAMAX|MX\s*ORIGINALS?|HOICHOI|ORIGINALS?)\b"
    r")",
    re.I,
)

TRAILING_JUNK_WORDS = frozenset({
    "hindi", "english", "eng", "tamil", "telugu", "malayalam", "kannada",
    "bengali", "bangla", "marathi", "punjabi", "korean", "japanese", "french",
    "spanish", "italian", "german", "russian", "thai", "filipino", "tagalog",
    "dub", "dubbed", "sub", "subbed", "esub", "msub", "dual", "multi", "audio",
    "org", "original", "originals", "web", "series", "ser", "movie",
    "movies", "film", "films", "short", "full", "all", "episodes", "episode",
    "ep", "season", "part", "vol", "volume", "complete", "combined", "uncut",
    "unrated", "uncensored", "extended", "remastered", "hot", "adult", "erotic",
    "bold", "desi", "xxx", "18", "18+", "hd", "fhd", "uhd", "4k", "hq", "sd",
    "480p", "720p", "1080p", "2160p", "hdrip", "webdl", "webrip", "bluray",
    "brrip", "bdrip", "dvdrip", "hdtv", "hevc", "x264", "x265", "h264", "h265",
    "aac", "ac3", "ddp", "dd5", "dd2", "dts", "atmos", "mkv", "mp4", "avi",
    "ullu", "altbalaji", "altt", "atrangii", "primeshots", "kooku", "voovi",
    "moodx", "hunters", "rabbit", "besharams", "hitprime", "hothit", "fliz",
    "nuefliks", "bigmoviezoo", "woow", "neonx", "hotx", "akkuott", "cineprime",
    "cheekuflix", "boommovies", "digimovieplex", "vivamax", "hoichoi", "mx",
    "nf", "amzn", "zee5", "sonyliv", "hotstar", "jio", "download", "watch",
    "online", "free", "new", "latest", "official", "release", "rip", "dl",
    "raw", "proper", "repack", "internal", "cut", "edition", "directors", "join",
})

LEADING_LABEL_RE = re.compile(
    r"^(?:title|name|movie(?:\s*name)?|series(?:\s*name)?|web\s*series|show|film)\s*[:\-–—=»>]+\s*",
    re.I,
)


def normalize_fancy_unicode(text: str) -> str:
    """Convert fancy mathematical/bold/italic Telegram Unicode text to plain ASCII/UTF-8."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", str(text))
    # Strip variation selectors and zero-width characters
    normalized = re.sub(r"[\u200b-\u200f\u202a-\u202e\ufeff\ufe0f]", "", normalized)
    return normalized


def normalize_title_tokens(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_fancy_unicode(str(value or "")).lower()).strip()


def is_generic_channel_or_junk_title(title: str) -> bool:
    norm = normalize_title_tokens(title)
    if not norm or len(norm) < 2:
        return True
    if norm in GENERIC_CHANNEL_TITLES:
        return True
    words = norm.split()
    if all(w in TRAILING_JUNK_WORDS or w.isdigit() for w in words):
        return True
    return False


def detect_ott_platform(*sources: str) -> str:
    combined = re.sub(r"[._]+", " ", normalize_fancy_unicode(" ".join(str(s or "") for s in sources)))
    for pattern, label in INDIAN_OTT_PLATFORMS:
        if re.search(pattern, combined, re.I):
            return label
    for pattern, label in GLOBAL_ADULT_STUDIOS:
        if re.search(pattern, combined, re.I):
            return label
    if re.search(r"\bvivamax\b", combined, re.I):
        return "Vivamax"
    return ""


def _strip_telegram_promo_lines(text: str) -> str:
    """Drop obvious promotional / channel-footer lines from multi-line Telegram captions."""
    if not text:
        return ""
    cleaned_lines: List[str] = []
    for raw_line in text.splitlines():
        line = normalize_fancy_unicode(raw_line).strip()
        if not line:
            continue
        lower = re.sub(r"^[^\w@#]+", "", line).strip().lower()
        # Skip pure promo / join / link / bot footer lines
        if re.match(r"^(?:join|subscribe|follow|backup|channel|powered\s*by|uploaded\s*by|credit|share|request|more\s*movies|t\.me/|https?://|@)", lower):
            continue
        if re.search(r"(?:join\s+(?:our|now|fast|here|channel|backup)|t\.me/|telegram\.me/)", lower) and not LEADING_LABEL_RE.match(line):
            # If the line is mostly promo, skip it
            stripped = re.sub(r"https?://\S+|t\.me/\S+|@\w+|#\w+", "", line).strip()
            if len(stripped) < 18:
                continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def _extract_structured_caption_field(text: str, field_regex: str) -> str:
    for line in text.splitlines():
        clean_line = re.sub(r"^[^\w@#]+", "", line).strip()
        m = re.match(rf"^(?:{field_regex})\s*[:\-–—=»>]+\s*(.+)$", clean_line, re.I)
        if m:
            return m.group(1).strip()
    return ""


def _extract_languages(text: str) -> str:
    spaced = re.sub(r"[._-]+", " ", text)
    langs: List[str] = []
    for pattern, label in LANGUAGE_PATTERNS:
        if re.search(pattern, spaced, re.I):
            if label not in langs:
                langs.append(label)
    return ", ".join(langs)


def _extract_extra_and_series_flags(text: str) -> Tuple[str, bool]:
    """
    Extract Season / Episode / Part / Volume / Edition markers and determine
    whether the text carries strong series structure.
    """
    text = re.sub(r"[_]+", " ", text)
    extras: List[str] = []
    has_season_or_ep = False

    compact_sp = re.search(r"\bS(?:EASON)?[\s._-]*0*(\d{1,2})[\s._-]*P(?:ART)?[\s._-]*0*(\d{1,3})\b", text, re.I)
    compact_se = re.search(
        r"\bS(?:EASON)?[\s._-]*0*(\d{1,2})[\s._-]*E(?:P(?:ISODE)?)?[\s._-]*0*(\d{1,3})(?:\s*(?:[-~_]|to)\s*(?:E|EP)?0*(\d{1,3}))?\b",
        text,
        re.I,
    )
    season_only = re.search(r"\bS(?:EASON)?[\s._-]*0*(\d{1,2})\b", text, re.I)
    ep_only = re.search(
        r"\b(?:EP(?:ISODE)?|E)[\s._-]*0*(\d{1,3})(?:\s*(?:[-~_]|to)\s*(?:E|EP)?0*(\d{1,3}))?\b",
        text,
        re.I,
    )
    part_only = re.search(r"(?:\b|(?<=\d))P(?:ART)?[\s._-]*0*(\d{1,3})\b", text, re.I)
    vol_only = re.search(r"\bVOL(?:UME)?\.?[\s._-]*([IVX]{1,4}|\d{1,2})\b", text, re.I)

    if compact_se:
        has_season_or_ep = True
        s_num = int(compact_se.group(1))
        e1 = int(compact_se.group(2))
        e2 = int(compact_se.group(3)) if compact_se.group(3) else None
        extras.append(f"S{s_num:02d}")
        extras.append(f"EP{e1:02d}-{e2:02d}" if e2 and e2 > e1 else f"EP{e1:02d}")
    elif compact_sp:
        has_season_or_ep = True
        extras.append(f"S{int(compact_sp.group(1)):02d}")
        extras.append(f"P{int(compact_sp.group(2)):02d}")
    else:
        if season_only:
            has_season_or_ep = True
            extras.append(f"S{int(season_only.group(1)):02d}")
        if ep_only:
            has_season_or_ep = True
            e1 = int(ep_only.group(1))
            e2 = int(ep_only.group(2)) if ep_only.group(2) else None
            extras.append(f"EP{e1:02d}-{e2:02d}" if e2 and e2 > e1 else f"EP{e1:02d}")
        if part_only:
            extras.append(f"P{int(part_only.group(1)):02d}")

    if vol_only and not any(x.startswith("P") for x in extras):
        extras.append(f"VOL {vol_only.group(1).upper()}")

    for tag in ("UNRATED", "UNCUT", "UNCENSORED", "EXTENDED", "COMPLETE", "COMBINED"):
        if re.search(rf"\b{tag}\b", text, re.I):
            extras.append(tag)

    if re.search(r"\b(?:all\s*episodes|full\s*season|web[\s._-]*series|web[\s._-]*se\b)\b", text, re.I):
        has_season_or_ep = True

    deduped = list(dict.fromkeys(extras))
    return " ".join(deduped), has_season_or_ep


def _clean_candidate_title_fragment(raw_fragment: str) -> str:
    """Clean a candidate title fragment while preserving real words and meaningful numbers."""
    text = normalize_fancy_unicode(raw_fragment)
    text = VIDEO_EXT_RE.sub("", text)
    text = re.sub(r"https?://\S+|t\.me/\S+|www\.[A-Za-z0-9.-]+\.[A-Za-z]{2,}", " ", text, flags=re.I)
    text = re.sub(r"@\w+|#\w+", " ", text)
    # Remove leading uploader brackets/braces like [Ullu], {@Channel}, (18+)
    text = re.sub(r"^(?:\s*(?:\[[^\]]*\]|\{[^}]*\}|\([^)]*\)|[^\w\s]+)\s*)+", "", text)
    text = LEADING_LABEL_RE.sub("", text)
    # Normalize dots and underscores to spaces first so \b word boundaries work on MoodX_Title_2025
    text = re.sub(r"[._|•★☆🔥🔞🎬🎥📺📥⚡✨💎❤️👉👇]+", " ", text)
    # Remove leading OTT/studio prefixes like "PrimeShots - Title", "MoodX Title", or "Ullu: Title"
    for pattern, _ in INDIAN_OTT_PLATFORMS + GLOBAL_ADULT_STUDIOS:
        text = re.sub(rf"^(?:{pattern})\s*[:\-–—|•/]*\s*", "", text, flags=re.I)
    # Strip any remaining bracketed technical tags like (2025), [720p], (Uncut)
    text = re.sub(r"[\[\(\{][^\]\)\}]{1,40}[\]\)\}]", " ", text)
    # Distinguish spaced subtitle hyphens ("Palang Tod - Gaon Ki Garmi") from word-separator hyphens ("Dekho-Magar-Pyar-Se")
    has_spaced_dash = bool(re.search(r"\s+[-–—]+\s+", text))
    if has_spaced_dash:
        text = re.sub(r"\s+[-–—]+\s+", " XSUBTITLEX ", text)
    # Tight hyphens (unless JAV code already handled) act as spaces in filenames
    text = re.sub(r"[-–—]+", " ", text)
    if has_spaced_dash:
        text = text.replace(" XSUBTITLEX ", " - ")
    text = re.sub(r"[^\w\s':\-&]", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -:&'")

    # Cut at the earliest technical/OTT/year token if there is a valid title before it
    split_match = TECH_SPLIT_RE.search(text)
    if split_match and split_match.start() >= 2:
        prefix = text[:split_match.start()].strip(" -:&'")
        if len(normalize_title_tokens(prefix)) >= 2 and not is_generic_channel_or_junk_title(prefix):
            text = prefix

    # Strip remaining standalone leading/trailing technical tokens
    words = text.split()
    while words and (
        words[0].lower().strip("-:&'") in TRAILING_JUNK_WORDS
        or words[0] in ("-", ":", "&")
    ):
        words.pop(0)
    while words and (
        words[-1].lower().strip("-:&'") in TRAILING_JUNK_WORDS
        or words[-1] in ("-", ":", "&")
    ):
        words.pop()

    cleaned = " ".join(words).strip(" -:&'")
    cleaned = re.sub(r"\s+-\s+$|^\s*-\s+", "", cleaned).strip()
    return cleaned


def parse_batch18_text(raw_text: str) -> Dict[str, Any]:
    """
    Parse a single raw filename or Telegram caption into structured identity fields.
    Handles:
      - Structured key-value Telegram captions (Title:, Year:, Audio:, etc.)
      - Messy release filenames with dots/underscores/hyphens/brackets/OTT tags
      - Adult movies (Hollywood, Bollywood, Korean, European, Filipino, etc.)
      - Adult OTT web series (Ullu, ALTBalaji, Atrangii, PrimeShots, Kooku, Voovi, etc.)
      - JAV studio codes (SSIS-123, ABP-456, FC2-PPV-123456)
    """
    original = normalize_fancy_unicode(str(raw_text or "")).strip()
    if not original:
        return {
            "title": "UNKNOWN",
            "year": "",
            "language": "",
            "extra_info": "",
            "platform": "",
            "content_type": "Movie",
            "jav_code": "",
            "confidence": 0,
            "raw": "",
        }

    cleaned_multiline = _strip_telegram_promo_lines(original)
    working = cleaned_multiline or original

    platform = detect_ott_platform(original)
    language = _extract_languages(original)
    extra_info, has_series_flags = _extract_extra_and_series_flags(original)

    # Check for explicit JAV code first
    jav_match = JAV_CODE_RE.search(original)
    jav_code = jav_match.group(1).upper().replace("_", "-") if jav_match else ""
    if jav_code and not language:
        language = "Japanese"

    # Extract year (preferring year not at index 0 unless it's structured `Year: 2025`)
    structured_year = _extract_structured_caption_field(working, r"year|released?|release\s*year")
    year = ""
    if structured_year:
        ym = re.search(r"\b((?:19|20)\d{2})\b", structured_year)
        if ym:
            year = ym.group(1)
    if not year:
        all_years = list(re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", working))
        # Prefer a year inside brackets `(2025)` or after the first word (avoids treating "2012" or "365" or "1920" title as year when another year exists)
        bracket_year = next((m for m in all_years if m.start() > 0 and working[m.start() - 1] in "([{"), None)
        if bracket_year:
            year = bracket_year.group(1)
        elif len(all_years) >= 2:
            year = all_years[-1].group(1)
        elif len(all_years) == 1 and all_years[0].start() > 1:
            year = all_years[0].group(1)

    # Extract title candidates in priority order:
    # 1. Explicit structured field (`Title: ...`, `Name: ...`, `Movie: ...`, `Series: ...`)
    # 2. First meaningful non-promo line
    # 3. Full text cleaned
    structured_title = _extract_structured_caption_field(
        working,
        r"title|name|movie(?:\s*name)?|series(?:\s*name)?|web\s*series|show|film",
    )

    candidate_sources: List[str] = []
    if structured_title:
        candidate_sources.append(structured_title)
    first_lines = [ln.strip() for ln in working.splitlines() if ln.strip()]
    if first_lines:
        candidate_sources.append(first_lines[0])
        if len(first_lines) > 1:
            candidate_sources.append(first_lines[1])
    candidate_sources.append(working)

    best_title = ""
    for src in candidate_sources:
        cleaned = _clean_candidate_title_fragment(src)
        if cleaned and not is_generic_channel_or_junk_title(cleaned):
            best_title = cleaned
            break

    if not best_title and jav_code:
        best_title = jav_code

    # Determine Movie vs Web Series accurately:
    # - Explicit Season/Episode/Web Series markers -> Web Series
    # - Known Indian adult OTT platforms (Ullu, PrimeShots, Kooku, Voovi, etc.) with Part/Episode or without explicit "Movie/Film" -> Web Series
    # - Otherwise -> Movie (handles Hollywood/Korean/European/Indian adult films!)
    indian_ott_names = {label for _, label in INDIAN_OTT_PLATFORMS}
    explicit_movie = bool(re.search(r"\b(?:full\s*movie|adult\s*movie|adult\s*film|short\s*film|feature\s*film|bluray|bdrip|dvdrip|theatrical|director'?s?\s*cut)\b", original, re.I))
    if has_series_flags:
        content_type = "Web Series"
    elif platform in indian_ott_names and not explicit_movie:
        content_type = "Web Series"
    else:
        content_type = "Movie"

    # Compute deterministic confidence score (0-100)
    confidence = 0
    if best_title and not is_generic_channel_or_junk_title(best_title):
        confidence += 55
        if structured_title:
            confidence += 15
        if year:
            confidence += 15
        if platform or language or extra_info:
            confidence += 10
        # Penalize suspiciously long/noisy titles (> 6 words)
        if len(best_title.split()) > 6:
            confidence -= 20

    return {
        "title": best_title or "UNKNOWN",
        "year": year,
        "language": language,
        "extra_info": extra_info,
        "platform": platform,
        "content_type": content_type,
        "jav_code": jav_code,
        "confidence": max(0, min(100, confidence)),
        "raw": original,
    }


def reconcile_batch18_identity(
    caption_raw: str = "",
    filename_raw: str = "",
    forward_title: str = "",
) -> Dict[str, Any]:
    """
    Intelligently fuse caption, filename, and forward channel evidence for /batch18.
    - Never lets generic channel names like 'UNRATED SERIES HD' hijack the title.
    - Prefers the cleaner, higher-confidence source (caption vs filename).
    - Merges year, language, platform, extra_info, and content_type across both sources.
    - Flags `needs_ai_reconcile=True` only when local evidence is weak or conflicting.
    """
    cap = parse_batch18_text(caption_raw)
    fn = parse_batch18_text(filename_raw)
    fwd_clean = _clean_candidate_title_fragment(forward_title) if forward_title else ""
    fwd_valid = bool(fwd_clean and not is_generic_channel_or_junk_title(fwd_clean))

    cap_ok = cap["title"] != "UNKNOWN" and not is_generic_channel_or_junk_title(cap["title"])
    fn_ok = fn["title"] != "UNKNOWN" and not is_generic_channel_or_junk_title(fn["title"])

    chosen_title = "UNKNOWN"
    chosen_source = "none"
    needs_ai = False

    if cap_ok and fn_ok:
        cap_norm = normalize_title_tokens(cap["title"])
        fn_norm = normalize_title_tokens(fn["title"])
        if cap_norm == fn_norm:
            # Prefer whichever has better casing (not ALL CAPS)
            chosen_title = cap["title"] if not cap["title"].isupper() else fn["title"]
            chosen_source = "caption+filename"
        elif cap_norm in fn_norm or fn_norm in cap_norm:
            # Prefer the cleaner/shorter non-junk title unless the longer one has a meaningful subtitle
            shorter, longer = (cap["title"], fn["title"]) if len(cap_norm) <= len(fn_norm) else (fn["title"], cap["title"])
            extra_words = set(normalize_title_tokens(longer).split()) - set(normalize_title_tokens(shorter).split())
            if extra_words and all(w in TRAILING_JUNK_WORDS for w in extra_words):
                chosen_title = shorter
            else:
                chosen_title = cap["title"] if cap["confidence"] >= fn["confidence"] else fn["title"]
            chosen_source = "caption+filename"
        else:
            # Genuine conflict between caption and filename — pick higher confidence AND flag for AI tie-break
            if fn["confidence"] > cap["confidence"] + 10:
                chosen_title = fn["title"]
                chosen_source = "filename"
            else:
                chosen_title = cap["title"]
                chosen_source = "caption"
            needs_ai = True
    elif cap_ok:
        chosen_title = cap["title"]
        chosen_source = "caption"
        needs_ai = cap["confidence"] < 55
    elif fn_ok:
        chosen_title = fn["title"]
        chosen_source = "filename"
        needs_ai = fn["confidence"] < 55
    elif fwd_valid:
        chosen_title = fwd_clean
        chosen_source = "forward"
        needs_ai = True
    else:
        needs_ai = True

    year = cap["year"] or fn["year"] or ""
    platform = cap["platform"] or fn["platform"] or detect_ott_platform(forward_title)
    jav_code = cap["jav_code"] or fn["jav_code"] or ""

    # Merge languages cleanly without duplicates
    merged_langs: List[str] = []
    for lang_csv in (cap["language"], fn["language"]):
        for part in lang_csv.split(","):
            p = part.strip()
            if p and p not in merged_langs:
                merged_langs.append(p)
    language = ", ".join(merged_langs) or "Hindi"

    # Merge extra_info tokens
    merged_extra: List[str] = []
    for ex in (cap["extra_info"], fn["extra_info"]):
        for token in ex.split():
            if token and token not in merged_extra:
                merged_extra.append(token)
    extra_info = " ".join(merged_extra)

    # Series wins if either source has concrete series structure
    if cap["content_type"] == "Web Series" or fn["content_type"] == "Web Series":
        content_type = "Web Series"
    else:
        content_type = "Movie"

    return {
        "title": chosen_title,
        "year": year,
        "language": language,
        "extra_info": extra_info,
        "platform": platform,
        "content_type": content_type,
        "jav_code": jav_code,
        "category": "Adult",
        "chosen_source": chosen_source,
        "needs_ai_reconcile": needs_ai,
        "caption_parsed": cap,
        "filename_parsed": fn,
    }


def build_batch18_search_queries(title: str, platform: str = "", jav_code: str = "") -> List[str]:
    """
    Generate ordered, deduplicated search queries to maximize recall across
    TMDB, IMDb Suggestion API, OMDb, TVMaze, and Wikipedia.
    """
    queries: List[str] = []

    def _add(q: str) -> None:
        cleaned = re.sub(r"\s+", " ", str(q or "")).strip(" -:&'")
        if len(cleaned) >= 2 and cleaned.lower() not in {x.lower() for x in queries}:
            queries.append(cleaned)

    base = str(title or "").strip()
    if jav_code:
        _add(jav_code)
    _add(base)

    # Handle colon/hyphen anthology titles (e.g., "Palang Tod - Gaon Ki Garmi" or "Charmsukh: Chawl House")
    if " - " in base or ":" in base:
        parts = [p.strip() for p in re.split(r"\s+-\s+|:\s*", base) if p.strip()]
        _add(" ".join(parts))
        for part in reversed(parts):  # Subtitle often has the exact IMDb/TMDB entry
            if len(normalize_title_tokens(part)) >= 3 and not is_generic_channel_or_junk_title(part):
                _add(part)

    # Try Hinglish spelling swaps while preserving original capitalization
    for pattern, replacement in HINGLISH_SPELLING_SWAPS:
        if re.search(pattern, base, re.I):
            def _match_case(m: re.Match) -> str:
                src = m.group(0)
                if src.isupper():
                    return replacement.upper()
                if src[:1].isupper():
                    return replacement.title()
                return replacement
            swapped = re.sub(pattern, _match_case, base, flags=re.I)
            _add(swapped)

    # Try stripping trailing numeric sequel/part suffix (e.g., "Charmsukh 2" -> "Charmsukh")
    stripped_num = re.sub(r"\s+(?:part\s*)?(?:\d+|[IVX]{1,4})$", "", base, flags=re.I).strip()
    if stripped_num and len(normalize_title_tokens(stripped_num)) >= 3:
        _add(stripped_num)

    # If title has '&', also try 'and'
    if "&" in base:
        _add(base.replace("&", "and"))

    return queries[:6]


def pick_best_imdb_suggestion(
    items: List[Dict[str, Any]],
    movie_name: str,
    movie_year: str = "",
    content_type_hint: str = "",
) -> Optional[Dict[str, Any]]:
    """Score IMDb suggestion items by title similarity, year agreement, type, and artwork."""
    target_norm = normalize_title_tokens(movie_name)
    if not target_norm:
        return None
    target_words = set(target_norm.split())
    hint_is_series = "series" in str(content_type_hint or "").lower()

    best_item: Optional[Dict[str, Any]] = None
    best_score = -1

    for item in items or []:
        imdb_id = str(item.get("id") or "")
        if not imdb_id.startswith("tt"):
            continue

        label_norm = normalize_title_tokens(item.get("l") or "")
        score = 0
        if label_norm == target_norm:
            score += 60
        elif target_norm in label_norm or (label_norm and label_norm in target_norm):
            score += 35
        elif target_words:
            label_words = set(label_norm.split())
            if label_words:
                score += int(35 * len(target_words & label_words) / len(target_words))

        y_val = item.get("y")
        if movie_year and str(movie_year).isdigit() and y_val:
            diff = abs(int(movie_year) - int(y_val))
            if diff == 0:
                score += 30
            elif diff <= 2:
                score += 12
            elif diff > 3:
                score -= 65

        qid = str(item.get("qid") or "").lower()
        if hint_is_series and qid in ("tvseries", "tvminiseries", "tvepisode"):
            score += 12
        elif not hint_is_series and content_type_hint and qid in ("movie", "video", "short"):
            score += 12

        if isinstance(item.get("i"), dict) and item["i"].get("imageUrl"):
            score += 10

        if score > best_score:
            best_score = score
            best_item = item

    return best_item if best_score >= 30 else None


def score_tmdb_adult_candidate(
    item: Dict[str, Any],
    search_queries: List[str],
    search_year: str = "",
    content_type_hint: str = "",
) -> int:
    """
    Score a TMDB movie/tv candidate fairly across ALL adult movies & web series
    (exact/alias title match, year agreement, media type preference, poster, votes).
    """
    names = [
        item.get("title"),
        item.get("name"),
        item.get("original_title"),
        item.get("original_name"),
    ]
    norm_names = [normalize_title_tokens(n) for n in names if n]
    if not norm_names:
        return -100

    title_score = 0
    for idx, q in enumerate(search_queries):
        q_norm = normalize_title_tokens(q)
        if not q_norm:
            continue
        q_words = set(q_norm.split())
        # Slightly prefer primary query over fallback variants
        variant_penalty = 0 if idx == 0 else 5
        for n_norm in norm_names:
            if n_norm == q_norm:
                title_score = max(title_score, 65 - variant_penalty)
            elif q_norm in n_norm or n_norm in q_norm:
                title_score = max(title_score, 42 - variant_penalty)
            elif q_words:
                n_words = set(n_norm.split())
                if n_words:
                    overlap = len(q_words & n_words) / max(len(q_words), 1)
                    if overlap >= 0.6:
                        title_score = max(title_score, int(40 * overlap) - variant_penalty)

    if title_score < 20:
        return -50

    score = title_score
    y_str = str(item.get("release_date") or item.get("first_air_date") or "")[:4]
    if search_year and str(search_year).strip().isdigit() and y_str.isdigit():
        diff = abs(int(str(search_year).strip()) - int(y_str))
        if diff == 0:
            score += 30
        elif diff == 1:
            score += 18
        elif diff <= 3:
            score += 6
        else:
            score -= 65

    media_type = str(item.get("media_type") or "").lower()
    hint_lower = str(content_type_hint or "").lower()
    if "series" in hint_lower and media_type == "tv":
        score += 12
    elif "movie" in hint_lower and media_type == "movie":
        score += 12

    if item.get("poster_path"):
        score += 10
    if item.get("overview"):
        score += 5
    if (item.get("vote_count") or 0) > 0:
        score += 3

    return score


def build_imdb_suggestion_url(movie_name: str) -> str:
    clean_first = re.sub(r"[^a-z0-9]", "", normalize_fancy_unicode(str(movie_name or "")).lower())[:1] or "x"
    return f"https://v3.sg.media-imdb.com/suggestion/{clean_first}/{quote(movie_name.strip())}.json"


def build_universal_adult_ai_prompt(
    *,
    movie_name: str,
    movie_year: str = "",
    movie_lang: str = "Hindi",
    content_type: str = "Movie",
    platform: str = "",
    evidence_context: str = "",
) -> str:
    """
    Build a universal, accurate metadata prompt covering ALL adult/18+ titles:
    Indian OTT web series, Indian adult films, Hollywood erotic films,
    Korean/Japanese/European/Filipino 18+ cinema, and unrated releases.
    """
    platform_line = platform or "Global / Indian Adult Cinema or OTT"
    year_hint = movie_year or "Unknown"
    media_label = "Web Series" if "series" in str(content_type or "").lower() else "Movie"

    return (
        f"You are an expert movie & web-series catalog metadata database covering ALL 18+/adult cinema "
        f"and OTT platforms worldwide (Hollywood, Bollywood, Korean, Japanese, European, Vivamax, "
        f"Ullu, ALTBalaji/ALTT, Atrangii, PrimeShots, Kooku, Voovi, MoodX, Hunters, Rabbit, Woow, NeonX, etc.).\n\n"
        f"Title: \"{movie_name}\"\n"
        f"Release Year Hint: {year_hint}\n"
        f"Language: {movie_lang or 'Hindi'}\n"
        f"Platform/Studio Hint: {platform_line}\n"
        f"Expected Format: {media_label}\n"
        f"Collected Public Evidence:\n{evidence_context or 'No extra public snippets available.'}\n\n"
        f"STRICT RULES:\n"
        f"- Identify the exact title matching the Release Year Hint ({year_hint}) if provided — never return an unrelated older film from a different decade.\n"
        f"- Keep \"content_type\" accurate: \"Movie\" for films/short films, \"Web Series\" for episodic OTT series.\n"
        f"- Provide a clean, factual 2-3 sentence plot summary (no explicit/pornographic descriptions; keep it catalog-safe), accurate genres, and real main cast if known (leave cast empty if unknown).\n"
        f"- Respond ONLY with valid JSON (no markdown, no backticks).\n\n"
        f"{{\n"
        f"  \"title\": \"{movie_name}\",\n"
        f"  \"year\": {movie_year if str(movie_year).isdigit() else 0},\n"
        f"  \"genre\": \"Adult, Romance, Drama\",\n"
        f"  \"rating\": \"18+\",\n"
        f"  \"plot\": \"2-3 line clean catalog story summary\",\n"
        f"  \"cast\": \"Actor1, Actor2\",\n"
        f"  \"content_type\": \"{media_label}\"\n"
        f"}}"
    )
