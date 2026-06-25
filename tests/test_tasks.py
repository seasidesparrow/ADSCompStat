"""
Unit tests for adscompstat/tasks.py.

Strategy
--------
tasks.py creates a Celery app and imports adscompstat.database / utils / match
at module-import time.  adscompstat.database in turn imports adscompstat.models
which uses ``sqlalchemy.ext.declarative.declarative_base`` — an import path
removed in SQLAlchemy 2.0.  To keep test_tasks.py independent of the installed
SQLAlchemy version (and of the real Celery broker), we inject lightweight mocks
into sys.modules for every module that tasks.py pulls in before allowing the
import to proceed.

The injections use a ``if module not in sys.modules`` guard so that if another
test file has already imported the real module earlier in the same pytest session
the real module is preserved; the per-test ``@patch`` decorators on
``adscompstat.tasks.*`` still work correctly in either case.

Inside each test we then patch ``adscompstat.tasks.db``,
``adscompstat.tasks.utils``, ``adscompstat.tasks.CrossrefMatcher``, and the
``.delay`` attributes of sibling tasks so tests are fully isolated.
"""

import json
import math
import sys
import unittest
from unittest.mock import MagicMock, call, patch


# ---------------------------------------------------------------------------
# Module-level mocking — must happen before ``from adscompstat import tasks``
# ---------------------------------------------------------------------------

def _make_task_decorator(**_kwargs):
    """Return a decorator that leaves the wrapped function unchanged."""
    return lambda f: f


_mock_app = MagicMock()
_mock_app.conf.get.return_value = None      # RECORDS_PER_BATCH, paths, etc.
_mock_app.exchange = MagicMock()
_mock_app.task = MagicMock(side_effect=_make_task_decorator)
_mock_app.logger = MagicMock()

_mock_app_module = MagicMock()
_mock_app_module.ADSCompStatCelery.return_value = _mock_app

# Inject stubs so tasks.py can be imported without real Celery, SQLAlchemy,
# or any other heavyweight dependency.
#
# NOTE: adscompstat.database, adscompstat.utils, and adscompstat.match are
# intentionally NOT injected here.  They are real importable modules (no heavy
# broker / external-service dependencies) and must remain real so that
# test_utils.py — collected alphabetically after test_tasks.py — gets the
# genuine module instead of a MagicMock.  The per-test @patch decorators
# replace tasks.db / tasks.utils / tasks.CrossrefMatcher within each test.
_INJECT = {
    "adscompstat.app": _mock_app_module,
}
for _mod in [
    "adsenrich",
    "adsenrich.bibcodes",
    "kombu",
]:
    if _mod not in sys.modules:
        _INJECT[_mod] = MagicMock()

for _mod, _mock in _INJECT.items():
    if _mod not in sys.modules:
        sys.modules[_mod] = _mock

from adscompstat import tasks  # noqa: E402  (import must follow sys.modules setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    filepath="/path/file.xml",
    doi="10.1234/test",
    issns='{"print": "0004-637X"}',
    bibdata='{"title": "Test Paper"}',
    classic_match="{}",
    status="Matched",
    matchtype="canonical",
    bibcode="2000ApJ...999..999Z",
    classic_bibcode="2000ApJ...999..999Z",
    notes="",
):
    return (filepath, doi, issns, bibdata, classic_match,
            status, matchtype, bibcode, classic_bibcode, notes)


def _make_summary_row(vol="1", fraction=0.9, count=100, by_year_json=None):
    """Return a row matching query_summary_single_bibstem output.

    Columns: (bibstem, volume, complete_fraction, paper_count, complete_by_year)
    ``complete_by_year`` stores per-year ADS/Crossref counts from
    get_completeness_fraction's ``by_year`` list.
    """
    if by_year_json is None:
        by_year_json = json.dumps([
            {"year": "2000", "ADS_records": 90, "Crossref_records": 100,
             "completeness": 0.9},
        ])
    return ("ApJ", vol, fraction, count, by_year_json)


# ---------------------------------------------------------------------------
# task_clear_classic_data
# ---------------------------------------------------------------------------

class TestTaskClearClassicData(unittest.TestCase):

    @patch("adscompstat.tasks.db")
    def test_success_calls_clear(self, mock_db):
        tasks.task_clear_classic_data()
        mock_db.clear_classic_data.assert_called_once_with(tasks.app)

    @patch("adscompstat.tasks.db")
    def test_exception_is_caught(self, mock_db):
        mock_db.clear_classic_data.side_effect = Exception("db error")
        tasks.task_clear_classic_data()  # must not propagate


# ---------------------------------------------------------------------------
# task_write_block
# ---------------------------------------------------------------------------

class TestTaskWriteBlock(unittest.TestCase):

    @patch("adscompstat.tasks.db")
    def test_success_calls_write_block(self, mock_db):
        table = MagicMock()
        data = [{"key": "val"}]
        tasks.task_write_block(table, data)
        mock_db.write_block.assert_called_once_with(tasks.app, table, data)

    @patch("adscompstat.tasks.db")
    def test_exception_is_caught(self, mock_db):
        mock_db.write_block.side_effect = Exception("write error")
        tasks.task_write_block(MagicMock(), [])


# ---------------------------------------------------------------------------
# task_write_matched_record_to_db
# ---------------------------------------------------------------------------

class TestTaskWriteMatchedRecordToDb(unittest.TestCase):

    @patch("adscompstat.tasks.db")
    def test_none_record_skips_db(self, mock_db):
        tasks.task_write_matched_record_to_db(None)
        mock_db.query_master_by_doi.assert_not_called()
        mock_db.write_matched_record.assert_not_called()

    @patch("adscompstat.tasks.db")
    def test_new_record_queries_and_writes(self, mock_db):
        mock_db.query_master_by_doi.return_value = []
        rec = _make_record()
        tasks.task_write_matched_record_to_db(rec)
        mock_db.query_master_by_doi.assert_called_once_with(tasks.app, rec[1])
        mock_db.write_matched_record.assert_called_once_with(tasks.app, [], rec)

    @patch("adscompstat.tasks.db")
    def test_existing_record_passes_result_to_write(self, mock_db):
        existing = [("row",)]
        mock_db.query_master_by_doi.return_value = existing
        rec = _make_record()
        tasks.task_write_matched_record_to_db(rec)
        mock_db.write_matched_record.assert_called_once_with(tasks.app, existing, rec)

    @patch("adscompstat.tasks.db")
    def test_db_exception_is_caught(self, mock_db):
        mock_db.query_master_by_doi.side_effect = Exception("query failed")
        tasks.task_write_matched_record_to_db(_make_record())


# ---------------------------------------------------------------------------
# task_process_logfile
# ---------------------------------------------------------------------------

class TestTaskProcessLogfile(unittest.TestCase):

    def _run(self, files, batch_count=100, harvest_dir="/harvest/"):
        def conf_get(key, default=None):
            if key == "RECORDS_PER_BATCH":
                return batch_count
            if key == "HARVEST_BASE_DIR":
                return harvest_dir
            return default

        with patch("adscompstat.tasks.app") as mock_app, \
             patch("adscompstat.tasks.utils") as mock_utils, \
             patch.object(tasks, "task_process_meta") as mock_meta:
            mock_app.conf.get.side_effect = conf_get
            mock_utils.read_updateagent_log.return_value = files
            mock_meta.delay = MagicMock()
            tasks.task_process_logfile("/some/logfile.log")
            return mock_meta.delay

    def test_empty_logfile_no_delay(self):
        delay = self._run([])
        delay.assert_not_called()

    def test_partial_batch_sends_one_delay(self):
        files = ["a.xml", "b.xml", "c.xml"]
        delay = self._run(files, batch_count=100, harvest_dir="/h/")
        delay.assert_called_once_with(["/h/a.xml", "/h/b.xml", "/h/c.xml"])

    def test_exact_batch_sends_one_delay(self):
        files = [f"file{i}.xml" for i in range(3)]
        delay = self._run(files, batch_count=3, harvest_dir="/h/")
        delay.assert_called_once()

    def test_overflow_sends_two_delays(self):
        files = [f"file{i}.xml" for i in range(5)]
        delay = self._run(files, batch_count=3, harvest_dir="/h/")
        self.assertEqual(delay.call_count, 2)
        first_batch = delay.call_args_list[0][0][0]
        second_batch = delay.call_args_list[1][0][0]
        self.assertEqual(len(first_batch), 3)
        self.assertEqual(len(second_batch), 2)

    def test_exception_is_caught(self):
        with patch("adscompstat.tasks.app") as mock_app, \
             patch("adscompstat.tasks.utils") as mock_utils:
            mock_app.conf.get.return_value = 100
            mock_utils.read_updateagent_log.side_effect = Exception("file not found")
            tasks.task_process_logfile("/missing.log")


# ---------------------------------------------------------------------------
# task_process_meta
# ---------------------------------------------------------------------------

class TestTaskProcessMeta(unittest.TestCase):

    def _run_meta(self, infile_batch, process_return=None, process_raise=None,
                  bibstem="ApJ..", bibcode="2000ApJ...999..999Z",
                  doi="10.1234/test", xmatch_result=None):
        """
        Run task_process_meta with mocked dependencies.

        Pass xmatch_result=None to use a default canonical hit.
        Pass xmatch_result={} to simulate an empty (falsy) match → NoIndex.
        """
        default_xmatch = {
            "match": "canonical",
            "bibcode": bibcode,
            "errs": {},
        }
        resolved_xmatch = default_xmatch if xmatch_result is None else xmatch_result

        with patch("adscompstat.tasks.utils") as mock_utils, \
             patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.BibcodeGenerator") as mock_bibgen_cls, \
             patch("adscompstat.tasks.CrossrefMatcher") as mock_matcher_cls, \
             patch.object(tasks, "task_write_matched_record_to_db") as mock_write:
            mock_write.delay = MagicMock()

            if process_raise:
                mock_utils.process_one_meta_xml.side_effect = process_raise
            else:
                mock_utils.process_one_meta_xml.return_value = (
                    process_return if process_return is not None else {}
                )

            mock_db.query_bibstem.return_value = bibstem
            mock_db.query_classic_bibcodes.return_value = ([], [])

            mock_bibgen = MagicMock()
            mock_bibgen.make_bibcode.return_value = bibcode
            mock_bibgen_cls.return_value = mock_bibgen

            mock_xmatch = MagicMock()
            mock_xmatch.match.return_value = resolved_xmatch
            mock_matcher_cls.return_value = mock_xmatch

            tasks.task_process_meta(infile_batch)
            return mock_write.delay

    def test_parse_exception_writes_failed_record(self):
        delay = self._run_meta(
            ["/path/bad.xml"],
            process_raise=Exception("parse error"),
        )
        delay.assert_called_once()
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "Failed")
        self.assertEqual(record[6], "failed")
        self.assertEqual(record[0], "/path/bad.xml")

    def test_parsestatus_set_writes_failed_record(self):
        process_return = {
            "status": "MissingDOI",
            "master_doi": "10.1234/x",
            "issns": {},
            "master_bibdata": {},
        }
        delay = self._run_meta(["/path/file.xml"], process_return=process_return)
        delay.assert_called_once()
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "Failed")
        self.assertEqual(record[9], "MissingDOI")

    def test_successful_canonical_match(self):
        process_return = {
            "status": "",
            "master_doi": "10.1234/test",
            "issns": {"print": "0004-637X"},
            "master_bibdata": {"title": "Test"},
            "record": {"publication": {"ISSN": []}},
        }
        delay = self._run_meta(
            ["/path/good.xml"],
            process_return=process_return,
            xmatch_result={"match": "canonical", "bibcode": "2000ApJ...999..999Z", "errs": {}},
        )
        delay.assert_called_once()
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "Matched")
        self.assertEqual(record[6], "canonical")

    def test_unmatched_result_sets_unmatched_status(self):
        process_return = {
            "status": "",
            "master_doi": "10.1234/test",
            "issns": {},
            "master_bibdata": {},
            "record": {},
        }
        delay = self._run_meta(
            ["/path/unmatched.xml"],
            process_return=process_return,
            xmatch_result={"match": "unmatched", "bibcode": None, "errs": {}},
        )
        delay.assert_called_once()
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "Unmatched")

    def test_no_xmatch_result_sets_no_index(self):
        """Empty dict is falsy → NoIndex path."""
        process_return = {
            "status": "",
            "master_doi": "10.1234/test",
            "issns": {},
            "master_bibdata": {},
            "record": {},
        }
        delay = self._run_meta(
            ["/path/noindex.xml"],
            process_return=process_return,
            xmatch_result={},   # falsy → xmatchResult is empty
        )
        delay.assert_called_once()
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "NoIndex")
        self.assertEqual(record[6], "other")

    def test_matching_exception_writes_failed_record(self):
        process_return = {
            "status": "",
            "master_doi": "10.1234/test",
            "issns": {},
            "master_bibdata": {},
            "record": {},
        }
        with patch("adscompstat.tasks.utils") as mock_utils, \
             patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.BibcodeGenerator") as mock_bibgen_cls, \
             patch.object(tasks, "task_write_matched_record_to_db") as mock_write:
            mock_write.delay = MagicMock()
            mock_utils.process_one_meta_xml.return_value = process_return
            mock_db.query_bibstem.side_effect = Exception("db error")
            mock_bibgen_cls.return_value = MagicMock()
            tasks.task_process_meta(["/path/err.xml"])
            mock_write.delay.assert_called_once()
            record = mock_write.delay.call_args[0][0]
            self.assertEqual(record[5], "Failed")
            self.assertEqual(record[6], "failed")

    def test_deleted_matchtype_maps_to_matched_status(self):
        process_return = {
            "status": "",
            "master_doi": "10.1234/test",
            "issns": {},
            "master_bibdata": {},
            "record": {},
        }
        delay = self._run_meta(
            ["/path/deleted.xml"],
            process_return=process_return,
            xmatch_result={"match": "deleted", "bibcode": "2000ApJ...999..999Z", "errs": {}},
        )
        record = delay.call_args[0][0]
        self.assertEqual(record[5], "Matched")
        self.assertEqual(record[6], "deleted")

    def test_batch_outer_exception_is_caught(self):
        # Passing a non-iterable should trigger the outer except
        tasks.task_process_meta(None)


# ---------------------------------------------------------------------------
# task_completeness_per_bibstem
# ---------------------------------------------------------------------------

class TestTaskCompletenessPerbibstem(unittest.TestCase):

    def _run(self, bibstem, db_result, completeness_bundle=None, write_raises=False):
        with patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.utils") as mock_utils:
            mock_db.query_completeness_per_bibstem.return_value = db_result
            mock_utils.get_completeness_fraction.return_value = completeness_bundle or {
                "volumeIndexable": 50,
                "volumeCompleteness": 0.85,
                "by_year": [{"year": "2000", "ADS_records": 43, "Crossref_records": 50}],
            }
            if write_raises:
                mock_db.write_completeness_summary.side_effect = Exception("write err")
            tasks.task_completeness_per_bibstem(bibstem)
            return mock_db, mock_utils

    def test_vol_ending_in_L_is_kept(self):
        # vol "099L" → last char is L → keep → lstrip/rstrip dots → "099L"
        db_result = [("099L", "2000", "Matched", "canonical", 10)]
        mock_db, _ = self._run("ApJ", db_result)
        mock_db.write_completeness_summary.assert_called_once()
        outrec = mock_db.write_completeness_summary.call_args[0][1]
        self.assertEqual(outrec[1], "099L")

    def test_vol_ending_in_P_is_kept(self):
        db_result = [("099P", "2000", "Matched", "canonical", 5)]
        mock_db, _ = self._run("ApJ", db_result)
        mock_db.write_completeness_summary.assert_called_once()
        outrec = mock_db.write_completeness_summary.call_args[0][1]
        self.assertEqual(outrec[1], "099P")

    def test_vol_ending_in_other_char_strips_last(self):
        # "0990" → last char '0' not in L/P → strip → "099" → lstrip/rstrip → "099"
        db_result = [("0990", "2000", "Matched", "canonical", 10)]
        mock_db, _ = self._run("ApJ", db_result)
        outrec = mock_db.write_completeness_summary.call_args[0][1]
        self.assertEqual(outrec[1], "099")

    def test_dot_prefix_stripped_from_vol(self):
        # "..990" → strip last → "..99" → lstrip('.') = "99"
        db_result = [("..990", "2000", "Matched", "canonical", 7)]
        mock_db, _ = self._run("ApJ", db_result)
        outrec = mock_db.write_completeness_summary.call_args[0][1]
        self.assertEqual(outrec[1], "99")

    def test_bibstem_padded_and_stored_stripped(self):
        # "ApJ" → padded to "ApJ.." → outrec[0] = "ApJ..".rstrip('.') = "ApJ"
        db_result = [("099", "2000", "Matched", "canonical", 5)]
        mock_db, _ = self._run("ApJ", db_result)
        outrec = mock_db.write_completeness_summary.call_args[0][1]
        self.assertEqual(outrec[0], "ApJ")

    def test_multiple_rows_same_vol_grouped(self):
        # Two rows with same stripped vol → volumeSummary[vol] has 2 entries
        db_result = [
            ("0990", "2000", "Matched", "canonical", 5),
            ("0990", "2000", "Unmatched", "unmatched", 2),
        ]
        mock_db, mock_utils = self._run("ApJ", db_result)
        bundle_arg = mock_utils.get_completeness_fraction.call_args[0][0]
        self.assertEqual(len(bundle_arg), 2)

    def test_db_query_exception_is_caught(self):
        with patch("adscompstat.tasks.db") as mock_db:
            mock_db.query_completeness_per_bibstem.side_effect = Exception("db err")
            tasks.task_completeness_per_bibstem("ApJ")

    def test_completeness_fraction_exception_is_caught(self):
        db_result = [("0990", "2000", "Matched", "canonical", 10)]
        with patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.utils") as mock_utils:
            mock_db.query_completeness_per_bibstem.return_value = db_result
            mock_utils.get_completeness_fraction.side_effect = Exception("calc err")
            tasks.task_completeness_per_bibstem("ApJ")
            mock_db.write_completeness_summary.assert_not_called()

    def test_write_exception_is_caught(self):
        db_result = [("0990", "2000", "Matched", "canonical", 10)]
        self._run("ApJ", db_result, write_raises=True)  # must not raise


# ---------------------------------------------------------------------------
# task_do_all_completeness
# ---------------------------------------------------------------------------

class TestTaskDoAllCompleteness(unittest.TestCase):

    def test_dispatches_per_bibstem_and_clears(self):
        with patch("adscompstat.tasks.db") as mock_db, \
             patch.object(tasks, "task_completeness_per_bibstem") as mock_per:
            mock_per.delay = MagicMock()
            mock_db.query_master_bibstems.return_value = [("ApJ",), ("AJ..",)]
            tasks.task_do_all_completeness()
            mock_db.clear_summary_data.assert_called_once_with(tasks.app)
            self.assertEqual(mock_per.delay.call_count, 2)
            calls = [c[0][0] for c in mock_per.delay.call_args_list]
            self.assertIn("ApJ", calls)
            self.assertIn("AJ..", calls)

    def test_empty_bibstems_skips_clear(self):
        with patch("adscompstat.tasks.db") as mock_db, \
             patch.object(tasks, "task_completeness_per_bibstem") as mock_per:
            mock_per.delay = MagicMock()
            mock_db.query_master_bibstems.return_value = []
            tasks.task_do_all_completeness()
            mock_db.clear_summary_data.assert_not_called()
            mock_per.delay.assert_not_called()

    def test_exception_is_caught(self):
        with patch("adscompstat.tasks.db") as mock_db:
            mock_db.query_master_bibstems.side_effect = Exception("db err")
            tasks.task_do_all_completeness()


# ---------------------------------------------------------------------------
# task_export_completeness_to_json
# ---------------------------------------------------------------------------

class TestTaskExportCompletenessToJson(unittest.TestCase):
    """
    task_export_completeness_to_json reads summary rows where r[4]
    (complete_by_year) is a JSON array of per-year dicts with keys:
      year, ADS_records, Crossref_records, completeness

    It builds a ``volumes`` dict keyed by year string, then restructures
    into ``volcomp`` = [{year: k, volumes: [...]}, ...] and computes
    ``earliestYear``/``latestYear`` via min/max on the year integers.
    """

    def _run(self, bibstems, rows_by_bib, export_file=None):
        """
        Run task_export_completeness_to_json, return (mock_db, mock_utils).

        ``rows_by_bib`` maps bibstem string → list of row tuples.
        """
        with patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.utils") as mock_utils, \
             patch("adscompstat.tasks.app") as mock_app:
            mock_app.conf.get.return_value = export_file
            mock_db.query_summary_bibstems.return_value = bibstems
            mock_db.query_summary_single_bibstem.side_effect = (
                lambda _app, bib: rows_by_bib.get(bib, [])
            )
            tasks.task_export_completeness_to_json()
            return mock_db, mock_utils

    def test_success_builds_alldata_and_calls_export(self):
        row = _make_summary_row()
        mock_db, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        mock_utils.export_completeness_data.assert_called_once()
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        self.assertEqual(len(alldata), 1)
        entry = alldata[0]
        self.assertEqual(entry["bibstem"], "ApJ")
        self.assertIn("title_completeness_fraction", entry)
        self.assertIn("completeness_details", entry)
        self.assertIn("earliest_year", entry)
        self.assertIn("latest_year", entry)
        self.assertEqual(entry["earliest_year"], 2000)
        self.assertEqual(entry["latest_year"], 2000)

    def test_vfrac_rounding_in_year_data(self):
        # ADS=9, Crossref=10 → vfrac = floor(10000 * 0.9 + 0.5) / 10000 = 0.9
        by_year = json.dumps([
            {"year": "2001", "ADS_records": 9, "Crossref_records": 10,
             "completeness": 0.9}
        ])
        row = _make_summary_row(by_year_json=by_year)
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        details = alldata[0]["completeness_details"]
        year2001 = next(x for x in details if x["year"] == "2001")
        self.assertAlmostEqual(
            year2001["volumes"][0]["completeness_fraction"], 0.9, places=4
        )

    def test_zero_crossref_count_gives_zero_vfrac(self):
        by_year = json.dumps([
            {"year": "2002", "ADS_records": 0, "Crossref_records": 0}
        ])
        row = _make_summary_row(by_year_json=by_year)
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        details = alldata[0]["completeness_details"]
        year2002 = next(x for x in details if x["year"] == "2002")
        self.assertEqual(year2002["volumes"][0]["completeness_fraction"], 0.0)

    def test_avg_completeness_floor_rounding(self):
        # fraction=0.123456789, count=10
        # averageCompleteness = 10 * 0.123456789 / 10 = 0.123456789
        # avg_export = floor(10000 * 0.123456789 + 0.5) / 10000
        fraction = 0.123456789
        expected_avg = math.floor(10000 * fraction + 0.5) / 10000.0
        row = _make_summary_row(fraction=fraction, count=10)
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        self.assertAlmostEqual(
            alldata[0]["title_completeness_fraction"], expected_avg, places=4
        )

    def test_integer_fraction_still_exported_correctly(self):
        # r[2] is int (not float) — branch: r2_export = r[2], avg still computed
        row = _make_summary_row(fraction=1, count=10)
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        self.assertIsNotNone(alldata[0]["title_completeness_fraction"])

    def test_multiple_years_earliest_and_latest(self):
        by_year = json.dumps([
            {"year": "1998", "ADS_records": 80, "Crossref_records": 100},
            {"year": "2005", "ADS_records": 90, "Crossref_records": 100},
        ])
        row = _make_summary_row(by_year_json=by_year)
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        alldata = mock_utils.export_completeness_data.call_args[0][0]
        self.assertEqual(alldata[0]["earliest_year"], 1998)
        self.assertEqual(alldata[0]["latest_year"], 2005)

    def test_invalid_years_json_triggers_exception_path(self):
        # Bare except in the function catches bad JSON for years; the entire
        # bib processing raises ZeroDivisionError (paperCount=0) which is
        # caught by the outer except → no export call
        row = _make_summary_row(count=0, by_year_json="not-json")
        _, mock_utils = self._run(["ApJ"], {"ApJ": [row]})
        # paperCount == 0 causes ZeroDivisionError → outer except → no export
        mock_utils.export_completeness_data.assert_not_called()

    def test_empty_bibstems_skips_export(self):
        _, mock_utils = self._run([], {})
        mock_utils.export_completeness_data.assert_not_called()

    def test_exception_at_query_is_caught(self):
        with patch("adscompstat.tasks.db") as mock_db, \
             patch("adscompstat.tasks.utils"), \
             patch("adscompstat.tasks.app"):
            mock_db.query_summary_bibstems.side_effect = Exception("db err")
            tasks.task_export_completeness_to_json()


# ---------------------------------------------------------------------------
# task_retry_records
# ---------------------------------------------------------------------------

class TestTaskRetryRecords(unittest.TestCase):

    def _run(self, db_rows, batch_count=100):
        def conf_get(key, default=None):
            if key == "RECORDS_PER_BATCH":
                return batch_count
            return default

        with patch("adscompstat.tasks.app") as mock_app, \
             patch("adscompstat.tasks.db") as mock_db, \
             patch.object(tasks, "task_process_meta") as mock_meta:
            mock_app.conf.get.side_effect = conf_get
            mock_meta.delay = MagicMock()
            mock_db.query_retry_files.return_value = db_rows
            tasks.task_retry_records("unmatched")
            return mock_meta.delay

    def test_empty_result_no_delay(self):
        delay = self._run([])
        delay.assert_not_called()

    def test_partial_batch_single_delay(self):
        rows = [("/path/a.xml",), ("/path/b.xml",)]
        delay = self._run(rows, batch_count=100)
        delay.assert_called_once_with(["/path/a.xml", "/path/b.xml"])

    def test_exact_batch_one_delay(self):
        rows = [(f"/path/{i}.xml",) for i in range(3)]
        delay = self._run(rows, batch_count=3)
        delay.assert_called_once()
        self.assertEqual(len(delay.call_args[0][0]), 3)

    def test_overflow_two_delays(self):
        rows = [(f"/path/{i}.xml",) for i in range(5)]
        delay = self._run(rows, batch_count=3)
        self.assertEqual(delay.call_count, 2)
        first = delay.call_args_list[0][0][0]
        second = delay.call_args_list[1][0][0]
        self.assertEqual(len(first), 3)
        self.assertEqual(len(second), 2)

    def test_exception_is_caught(self):
        with patch("adscompstat.tasks.app") as mock_app, \
             patch("adscompstat.tasks.db") as mock_db:
            mock_app.conf.get.return_value = 100
            mock_db.query_retry_files.side_effect = Exception("query error")
            tasks.task_retry_records("failed")


if __name__ == "__main__":
    unittest.main()
