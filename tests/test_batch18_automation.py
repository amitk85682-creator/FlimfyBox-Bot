import unittest

from services.batch18_automation import (
    Batch18Collector,
    Batch18RunSummary,
    Batch18TitleOutcome,
    MAX_BATCH18_FILES,
    build_batch18_title_groups,
    compute_batch18_backoff,
    format_batch18_final_report,
    is_batch18_song_junk,
    is_retryable_batch18_error,
)


class TestBatch18AutomationCollector(unittest.TestCase):
    def test_enforces_400_file_cap_strictly(self):
        self.assertEqual(MAX_BATCH18_FILES, 400)
        collector = Batch18Collector()
        for i in range(400):
            res = collector.add_file(
                file_id=f"fid_{i}",
                file_unique_id=f"uid_{i}",
                file_name=f"Title_{i // 4}_(2025)_S01E{(i % 4) + 1:02d}_720p_ULLU.mp4",
                file_size=100_000 + i,
            )
            self.assertTrue(res.accepted)
            self.assertEqual(res.reason, "accepted")
            self.assertEqual(res.count, i + 1)

        overflow = collector.add_file(
            file_id="fid_401",
            file_unique_id="uid_401",
            file_name="Extra_Movie_(2025)_1080p.mp4",
            file_size=500_000,
        )
        self.assertFalse(overflow.accepted)
        self.assertEqual(overflow.reason, "limit_reached")
        self.assertEqual(overflow.count, 400)

    def test_duplicate_protection_catches_unique_id_and_name_size(self):
        collector = Batch18Collector()
        first = collector.add_file(
            file_id="f1",
            file_unique_id="u_same",
            file_name="Dekho-Magar-Pyar-Se-(2025)-Hindi-Part-1-ULLU.mp4",
            file_size=320_000_000,
        )
        self.assertTrue(first.accepted)

        # Duplicate by Telegram file_unique_id
        dup_uid = collector.add_file(
            file_id="f2",
            file_unique_id="u_same",
            file_name="Different_Name.mp4",
            file_size=999_999,
        )
        self.assertFalse(dup_uid.accepted)
        self.assertEqual(dup_uid.reason, "duplicate")

        # Duplicate by identical normalized filename + file_size (re-uploaded)
        dup_meta = collector.add_file(
            file_id="f3",
            file_unique_id="u_new",
            file_name="  dekho-magar-pyar-se-(2025)-hindi-part-1-ullu.mp4  ",
            file_size=320_000_000,
        )
        self.assertFalse(dup_meta.accepted)
        self.assertEqual(dup_meta.reason, "duplicate")
        self.assertEqual(collector.duplicates_skipped, 2)
        self.assertEqual(len(collector.records), 1)

    def test_song_junk_filter_is_conservative_for_adult_short_films(self):
        self.assertTrue(is_batch18_song_junk("Hot_Item_Full_Video_Song_1080p.mp4"))
        self.assertTrue(is_batch18_song_junk("Title_Track.mp3"))
        self.assertTrue(is_batch18_song_junk("Movie_Official_Trailer_2025.mp4"))

        # Short adult OTT episodes and films MUST NOT be skipped
        self.assertFalse(is_batch18_song_junk("Charmsukh_Short_Film_(2024)_720p_ULLU.mp4"))
        self.assertFalse(
            is_batch18_song_junk(
                "Dekho_Magar_Pyar_Se_S01E02_720p.mp4",
                caption="Includes romantic song scene",
            )
        )


class TestBatch18TitleGrouping(unittest.TestCase):
    def test_single_file_single_movie_group(self):
        collector = Batch18Collector()
        collector.add_file(
            file_id="f1",
            file_unique_id="u1",
            file_name="365.Days.This.Day.2022.1080p.NF.WEB-DL.Dual.Audio.mkv",
            file_size=1_500_000_000,
        )
        groups = build_batch18_title_groups(collector.records)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["title"], "365 Days This Day")
        self.assertEqual(groups[0]["year"], "2022")
        self.assertEqual(groups[0]["content_type"], "Movie")
        self.assertEqual(groups[0]["file_count"], 1)

    def test_multi_title_grouping_merges_missing_year_and_separates_different_years(self):
        collector = Batch18Collector()
        inputs = [
            # Series A: Part 2 arrives first, Part 1 arrives later without year
            ("u1", "Dekho-Magar-Pyar-Se-(2025)-Hindi-Season-1-Part-2-ULLU-720p.mp4", "", 220_000_000),
            ("u2", "Dekho-Magar-Pyar-Se-Season-1-Part-1-ULLU-480p.mp4", "", 150_000_000),
            # Movie B: 2015 vs 2024 same title must stay separate!
            ("u3", "Love.2015.1080p.UNRATED.BluRay.mkv", "", 1_800_000_000),
            ("u4", "Love.2024.720p.WEB-DL.mkv", "", 950_000_000),
            # Series C: Korean / ALTBalaji multi-episode out of order
            ("u5", "Gandii.Baat.S02E03.720p.ALTBalaji.mkv", "", 400_000_000),
            ("u6", "Gandii.Baat.S02E01.720p.ALTBalaji.mkv", "", 390_000_000),
            ("u7", "Gandii.Baat.S02E02.720p.ALTBalaji.mkv", "", 395_000_000),
        ]
        for uid, fname, cap, sz in inputs:
            res = collector.add_file(
                file_id=f"id_{uid}",
                file_unique_id=uid,
                file_name=fname,
                file_size=sz,
                caption=cap,
            )
            self.assertTrue(res.accepted)

        groups = build_batch18_title_groups(collector.records)
        self.assertEqual(len(groups), 4)

        # Group 1: Dekho Magar Pyar Se (2025) with Part 1 sorted before Part 2
        g0 = groups[0]
        self.assertEqual(g0["title"], "Dekho Magar Pyar Se")
        self.assertEqual(g0["year"], "2025")
        self.assertEqual(g0["content_type"], "Web Series")
        self.assertEqual(g0["file_count"], 2)
        self.assertIn("Part-1", g0["files"][0]["file_name"])
        self.assertIn("Part-2", g0["files"][1]["file_name"])

        # Groups 2 & 3: Love (2015) and Love (2024) kept strictly separate
        self.assertEqual(groups[1]["title"], "Love")
        self.assertEqual(groups[1]["year"], "2015")
        self.assertEqual(groups[2]["title"], "Love")
        self.assertEqual(groups[2]["year"], "2024")

        # Group 4: Gandii Baat sorted E01 -> E02 -> E03
        g3 = groups[3]
        self.assertEqual(g3["title"], "Gandii Baat")
        self.assertEqual(g3["content_type"], "Web Series")
        self.assertEqual(
            [f["file_name"] for f in g3["files"]],
            [
                "Gandii.Baat.S02E01.720p.ALTBalaji.mkv",
                "Gandii.Baat.S02E02.720p.ALTBalaji.mkv",
                "Gandii.Baat.S02E03.720p.ALTBalaji.mkv",
            ],
        )

    def test_400_files_across_100_titles_groups_cleanly(self):
        collector = Batch18Collector()
        for t in range(100):
            for ep in range(4):
                res = collector.add_file(
                    file_id=f"f_{t}_{ep}",
                    file_unique_id=f"u_{t}_{ep}",
                    file_name=f"AdultShow{t:03d}.(2025).S01E{ep + 1:02d}.720p.ULLU.mp4",
                    file_size=100_000 + t * 10 + ep,
                )
                self.assertTrue(res.accepted)

        groups = build_batch18_title_groups(collector.records)
        self.assertEqual(len(groups), 100)
        self.assertTrue(all(g["file_count"] == 4 for g in groups))
        self.assertTrue(all(g["content_type"] == "Web Series" for g in groups))


class TestBatch18RetryAndReporting(unittest.TestCase):
    def test_retry_classification_and_backoff_math(self):
        class FakeRetryAfter(Exception):
            pass

        self.assertTrue(is_retryable_batch18_error(FakeRetryAfter("Flood control exceeded")))
        self.assertTrue(is_retryable_batch18_error(TimeoutError("timed out")))
        self.assertTrue(is_retryable_batch18_error(ConnectionError("reset")))
        self.assertFalse(is_retryable_batch18_error(ValueError("bad title")))

        self.assertEqual(compute_batch18_backoff(1, retry_after=12), 13.0)
        self.assertEqual(compute_batch18_backoff(1, retry_after=999), 65.0)
        self.assertEqual(compute_batch18_backoff(1), 1.5)
        self.assertEqual(compute_batch18_backoff(2), 3.0)
        self.assertEqual(compute_batch18_backoff(3), 6.0)
        self.assertEqual(compute_batch18_backoff(6), 20.0)

    def test_final_report_escapes_html_and_respects_telegram_length_limit(self):
        outcomes = [
            Batch18TitleOutcome(
                title=f"Title <{i}> & Co",
                year="2025",
                files_total=2,
                files_saved=2 if i % 5 != 0 else 0,
                files_failed=0 if i % 5 != 0 else 2,
                posted=(i % 5 != 0),
                error="" if i % 5 != 0 else "RuntimeError: <timeout>",
            )
            for i in range(1, 85)
        ]
        summary = Batch18RunSummary(
            batch_id="b18_test123456",
            collected_files=168,
            in_batch_duplicates=4,
            junk_skipped=1,
            duration_seconds=125.4,
            title_outcomes=outcomes,
        )
        report = format_batch18_final_report(summary)
        self.assertLessEqual(len(report), 3900)
        self.assertIn("b18_test123456", report)
        self.assertIn("&lt;1&gt; &amp; Co", report)
        self.assertIn("/retry18", report)
        self.assertIn("aur title(s) process hue", report)


if __name__ == "__main__":
    unittest.main()
