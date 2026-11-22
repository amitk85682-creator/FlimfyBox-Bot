"""Unit tests for P1 Supabase RLS remediation, P3 modular services, and Smart /batch18 Engine."""

import sys
import types
import unittest

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    _pg_stub = types.ModuleType("psycopg2")
    _pg_stub.Error = Exception
    _pg_extras = types.ModuleType("psycopg2.extras")
    sys.modules["psycopg2"] = _pg_stub
    sys.modules["psycopg2.extras"] = _pg_extras

import db_migrations
from services.api_contracts import error_payload, health_payload, ok_payload, resolve_data_mode
from services.batch18_metadata import (
    build_batch18_search_queries,
    build_imdb_suggestion_url,
    build_universal_adult_ai_prompt,
    detect_ott_platform,
    is_generic_channel_or_junk_title,
    parse_batch18_text,
    pick_best_imdb_suggestion,
    reconcile_batch18_identity,
    score_tmdb_adult_candidate,
)
from services.rls_audit import (
    ALL_AUDITED_PUBLIC_TABLES,
    build_rls_remediation_statements,
    validate_table_identifier,
)


class RlsAuditAndMigrationTest(unittest.TestCase):
    def test_all_audited_tables_present_and_unique(self):
        self.assertEqual(len(ALL_AUDITED_PUBLIC_TABLES), 46)
        self.assertEqual(len(set(ALL_AUDITED_PUBLIC_TABLES)), 46)
        for required in (
            "movies",
            "movie_files",
            "temp_links",
            "global_chat_messages",
            "user_watchlist",
            "batch18_jobs",
            "batch18_job_items",
        ):
            self.assertIn(required, ALL_AUDITED_PUBLIC_TABLES)

    def test_rls_statements_enable_rls_and_revoke_direct_roles(self):
        statements = build_rls_remediation_statements()
        self.assertEqual(len(statements), 46)
        sample = statements[0]
        self.assertIn("ENABLE ROW LEVEL SECURITY", sample)
        self.assertIn("REVOKE ALL ON TABLE", sample)
        self.assertIn("FROM anon", sample)
        self.assertIn("FROM authenticated", sample)

    def test_validate_table_identifier_blocks_sql_injection(self):
        self.assertEqual(validate_table_identifier("temp_links"), "temp_links")
        with self.assertRaises(ValueError):
            validate_table_identifier("movies; DROP TABLE users;--")

    def test_migrations_21_and_22_are_registered(self):
        versions = [v for v, _ in db_migrations.MIGRATIONS]
        self.assertIn(21, versions)
        self.assertIn(22, versions)
        self.assertEqual(len(versions), len(set(versions)))


class Batch18SmartParserAndLookupTest(unittest.TestCase):
    def test_indian_ott_ullu_filename_parsing(self):
        parsed = parse_batch18_text("Dekho-Magar-Pyar-Se-(2025)-Hindi-Season-1-Part-2-ULLU-WEB-Se.mp4")
        self.assertEqual(parsed["title"], "Dekho Magar Pyar Se")
        self.assertEqual(parsed["year"], "2025")
        self.assertEqual(parsed["platform"], "Ullu")
        self.assertEqual(parsed["content_type"], "Web Series")
        self.assertIn("S01", parsed["extra_info"])
        self.assertIn("P02", parsed["extra_info"])
        self.assertIn("Hindi", parsed["language"])

    def test_strips_channel_handles_ott_prefixes_and_anthology_subtitles(self):
        parsed = parse_batch18_text(
            "🔥 @AdultHub_18Plus [Ullu] Palang Tod - Gaon Ki Garmi (2023) S01 P02 Uncut 1080p.mkv"
        )
        self.assertEqual(parsed["title"], "Palang Tod - Gaon Ki Garmi")
        self.assertEqual(parsed["year"], "2023")
        self.assertEqual(parsed["platform"], "Ullu")
        self.assertEqual(parsed["content_type"], "Web Series")

        variants = build_batch18_search_queries(parsed["title"], parsed["platform"])
        self.assertIn("Gaon Ki Garmi", variants)
        self.assertIn("Palang Tod", variants)

    def test_primeshots_prefix_removed_from_title(self):
        parsed = parse_batch18_text("PrimeShots - Pyaasi Bhabhi (2024) 720p Unrated HDRip.mkv")
        self.assertEqual(parsed["title"], "Pyaasi Bhabhi")
        self.assertEqual(parsed["platform"], "PrimeShots")
        self.assertEqual(parsed["year"], "2024")
        self.assertEqual(parsed["content_type"], "Web Series")

    def test_hollywood_and_international_adult_movies_classified_as_movie(self):
        hollywood = parse_batch18_text("365.Days.This.Day.2022.1080p.NF.WEB-DL.Dual.Audio.Hindi.English.mkv")
        self.assertEqual(hollywood["title"], "365 Days This Day")
        self.assertEqual(hollywood["year"], "2022")
        self.assertEqual(hollywood["content_type"], "Movie")
        self.assertIn("Hindi", hollywood["language"])
        self.assertIn("English", hollywood["language"])

        korean = parse_batch18_text("The.Handmaiden.2016.Extended.Korean.1080p.BluRay.x264.mkv")
        self.assertEqual(korean["title"], "The Handmaiden")
        self.assertEqual(korean["year"], "2016")
        self.assertEqual(korean["content_type"], "Movie")
        self.assertIn("Korean", korean["language"])
        self.assertIn("EXTENDED", korean["extra_info"])

        euro = parse_batch18_text("Nymphomaniac.Vol.I.2013.Directors.Cut.1080p.BluRay.mkv")
        self.assertEqual(euro["title"], "Nymphomaniac")
        self.assertEqual(euro["year"], "2013")
        self.assertEqual(euro["content_type"], "Movie")
        self.assertIn("VOL I", euro["extra_info"])

    def test_structured_multiline_caption_with_promo_links_and_fancy_unicode(self):
        caption = (
            "Join @SpamBackup https://t.me/spam\n"
            "🎬 Title: 𝐋𝐨𝐯𝐞 (2015)\n"
            "🔊 Audio: English + Hindi\n"
            "📥 Quality: 1080p Unrated\n"
            "Uploaded by @RandomBot"
        )
        parsed = parse_batch18_text(caption)
        self.assertEqual(parsed["title"], "Love")
        self.assertEqual(parsed["year"], "2015")
        self.assertEqual(parsed["content_type"], "Movie")
        self.assertIn("English", parsed["language"])
        self.assertIn("Hindi", parsed["language"])

    def test_jav_code_extraction(self):
        parsed = parse_batch18_text("[Uncensored] SSIS-482 1080p HD.mp4")
        self.assertEqual(parsed["jav_code"], "SSIS-482")
        self.assertEqual(parsed["language"], "Japanese")
        self.assertIn("UNCENSORED", parsed["extra_info"])

    def test_reconcile_ignores_generic_forward_channel_and_merges_caption_filename(self):
        self.assertTrue(is_generic_channel_or_junk_title("UNRATED SERIES HD"))
        rec = reconcile_batch18_identity(
            caption_raw="🔥 Join @AdultZone\nUNRATED SERIES HD\n720p Hindi",
            filename_raw="Dekho-Magar-Pyar-Se-(2025)-Hindi-Season-1-Part-2-ULLU-WEB-Se.mp4",
            forward_title="UNRATED SERIES HD",
        )
        self.assertEqual(rec["title"], "Dekho Magar Pyar Se")
        self.assertEqual(rec["year"], "2025")
        self.assertEqual(rec["platform"], "Ullu")
        self.assertEqual(rec["content_type"], "Web Series")
        self.assertFalse(rec["needs_ai_reconcile"])

    def test_hinglish_query_expansion(self):
        variants = build_batch18_search_queries("Dekho Magar Pyar Se")
        self.assertIn("Dekho Magar Pyar Se", variants)
        self.assertIn("Dekho Magar Pyaar Se", variants)

    def test_imdb_suggestion_picks_matching_year_over_older_namesake(self):
        candidates = [
            {"id": "tt1493357", "l": "Dekho Magar Pyaar Se", "y": 2004, "qid": "tvSeries", "i": {"imageUrl": "https://img/old.jpg"}},
            {"id": "tt35610472", "l": "Dekho Magar Pyar Se", "y": 2025, "qid": "tvSeries", "i": {"imageUrl": "https://img/new.jpg"}},
        ]
        picked = pick_best_imdb_suggestion(candidates, "Dekho Magar Pyar Se", "2025", content_type_hint="Web Series")
        self.assertIsNotNone(picked)
        self.assertEqual(picked["id"], "tt35610472")

    def test_tmdb_candidate_scoring_prefers_movie_match_over_unrelated_tv_hit(self):
        movie_hit = {
            "id": 292431,
            "media_type": "movie",
            "title": "Love",
            "release_date": "2015-07-15",
            "poster_path": "/love2015.jpg",
            "overview": "Murphy is an American living in Paris...",
            "vote_count": 2600,
        }
        wrong_tv_hit = {
            "id": 99999,
            "media_type": "tv",
            "name": "Love Island",
            "first_air_date": "2015-06-07",
            "poster_path": "/island.jpg",
            "overview": "Reality dating show",
            "vote_count": 100,
        }
        queries = build_batch18_search_queries("Love")
        s_movie = score_tmdb_adult_candidate(movie_hit, queries, "2015", "Movie")
        s_tv = score_tmdb_adult_candidate(wrong_tv_hit, queries, "2015", "Movie")
        self.assertGreater(s_movie, s_tv)

    def test_imdb_suggestion_url_and_platform_detection_and_universal_prompt(self):
        url = build_imdb_suggestion_url("Dekho Magar Pyar Se")
        self.assertTrue(url.startswith("https://v3.sg.media-imdb.com/suggestion/d/"))
        self.assertEqual(detect_ott_platform("Dekho-Magar-Pyar-Se-2025-ULLU-Web-Series.mp4"), "Ullu")
        self.assertEqual(detect_ott_platform("MoodX_Raat_Ki_Rani_2025.mkv"), "MoodX")

        movie_prompt = build_universal_adult_ai_prompt(
            movie_name="The Handmaiden",
            movie_year="2016",
            movie_lang="Korean",
            content_type="Movie",
        )
        self.assertIn('"content_type": "Movie"', movie_prompt)
        self.assertNotIn("This is a web series, NOT an old film", movie_prompt)


class ApiContractsTest(unittest.TestCase):
    def test_standard_envelopes(self):
        ok = ok_payload({"count": 3}, request_id="req-1")
        self.assertEqual(ok["status"], "success")
        self.assertEqual(ok["count"], 3)

        err = error_payload("AUTH_REQUIRED", "Sign in required", request_id="req-2")
        self.assertEqual(err, {
            "status": "error",
            "code": "AUTH_REQUIRED",
            "message": "Sign in required",
            "request_id": "req-2",
        })

        health = health_payload(service="flimfybox-bot", db_connected=True, uptime_seconds=12.345)
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["data_mode"], resolve_data_mode(True))


if __name__ == "__main__":
    unittest.main()
