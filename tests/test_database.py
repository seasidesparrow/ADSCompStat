import unittest
from unittest.mock import MagicMock, call, patch

from adscompstat import database as db
from adscompstat.database import (
    BibstemLookupException,
    DBClearClassicException,
    DBClearSummaryException,
    DBQueryException,
    DBWriteException,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_app():
    """Return (mock_app, mock_session) where session_scope() is wired up."""
    mock_app = MagicMock()
    mock_session = MagicMock()
    mock_app.session_scope.return_value.__enter__.return_value = mock_session
    mock_app.session_scope.return_value.__exit__.return_value = False
    return mock_app, mock_session


def _make_matched_record(
    filepath="/path/to/file.xml",
    doi="10.1234/abc",
    issns='{"electronic": "1538-4357"}',
    bibdata='{"title": "A Paper"}',
    classic_match="{}",
    status="Matched",
    matchtype="canonical",
    bibcode_meta="2000ApJ...999..999Z",
    bibcode_classic="2000ApJ...999..999Z",
    notes="",
):
    return (filepath, doi, issns, bibdata, classic_match,
            status, matchtype, bibcode_meta, bibcode_classic, notes)


# ---------------------------------------------------------------------------
# query_bibstem
# ---------------------------------------------------------------------------

class TestQueryBibstem(unittest.TestCase):

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_hyphenated_issn_passed_as_is(self, mock_lookup):
        mock_lookup.return_value = ("ApJ",)
        record = {"publication": {"ISSN": [{"issnString": "0004-637X"}]}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "ApJ")
        mock_lookup.assert_called_once()
        args = mock_lookup.call_args[0]
        self.assertEqual(args[1], "0004-637X")

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_unhyphenated_issn_gets_hyphen_inserted(self, mock_lookup):
        # 8-char ISSN without hyphen → hyphen inserted before lookup
        mock_lookup.return_value = ("ApJ",)
        record = {"publication": {"ISSN": [{"issnString": "0004637X"}]}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "ApJ")
        args = mock_lookup.call_args[0]
        self.assertEqual(args[1], "0004-637X")

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_empty_issn_list_returns_empty_string(self, mock_lookup):
        record = {"publication": {"ISSN": []}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "")
        mock_lookup.assert_not_called()

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_no_publication_key_returns_empty_string(self, mock_lookup):
        result = db.query_bibstem(MagicMock(), {})
        self.assertEqual(result, "")
        mock_lookup.assert_not_called()

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_no_bibstem_match_returns_empty_string(self, mock_lookup):
        mock_lookup.return_value = None
        record = {"publication": {"ISSN": [{"issnString": "0000-0000"}]}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "")

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_stops_after_first_match(self, mock_lookup):
        # Only the first ISSN that yields a result should be used
        mock_lookup.return_value = ("ApJ",)
        record = {"publication": {"ISSN": [
            {"issnString": "0004-637X"},
            {"issnString": "1538-4357"},
        ]}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "ApJ")
        mock_lookup.assert_called_once()

    @patch("adscompstat.database.query_bibstem_by_issn")
    def test_skips_empty_issn_string(self, mock_lookup):
        # An ISSN entry with an empty string should be silently skipped
        mock_lookup.return_value = ("ApJ",)
        record = {"publication": {"ISSN": [
            {"issnString": ""},
            {"issnString": "0004-637X"},
        ]}}
        result = db.query_bibstem(MagicMock(), record)
        self.assertEqual(result, "ApJ")
        # lookup only called for the non-empty ISSN
        args = mock_lookup.call_args[0]
        self.assertEqual(args[1], "0004-637X")


# ---------------------------------------------------------------------------
# clear_classic_data
# ---------------------------------------------------------------------------

class TestClearClassicData(unittest.TestCase):

    def test_success_deletes_three_tables(self):
        mock_app, mock_session = make_mock_app()
        db.clear_classic_data(mock_app)
        self.assertEqual(mock_session.query.call_count, 3)
        mock_session.commit.assert_called_once()

    def test_exception_raises_db_clear_classic_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.return_value.delete.side_effect = Exception("DB error")
        with self.assertRaises(DBClearClassicException):
            db.clear_classic_data(mock_app)
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


# ---------------------------------------------------------------------------
# clear_summary_data
# ---------------------------------------------------------------------------

class TestClearSummaryData(unittest.TestCase):

    def test_success(self):
        mock_app, mock_session = make_mock_app()
        db.clear_summary_data(mock_app)
        mock_session.query.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_exception_raises_db_clear_summary_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.return_value.delete.side_effect = Exception("DB error")
        with self.assertRaises(DBClearSummaryException):
            db.clear_summary_data(mock_app)
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


# ---------------------------------------------------------------------------
# query_master_by_doi
# ---------------------------------------------------------------------------

class TestQueryMasterByDoi(unittest.TestCase):

    def test_returns_query_result(self):
        mock_app, mock_session = make_mock_app()
        expected = [("10.1234/abc",)]
        mock_session.query.return_value.filter_by.return_value.all.return_value = expected
        result = db.query_master_by_doi(mock_app, "10.1234/abc")
        self.assertEqual(result, expected)

    def test_empty_result(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.return_value.filter_by.return_value.all.return_value = []
        result = db.query_master_by_doi(mock_app, "10.9999/zzz")
        self.assertEqual(result, [])

    def test_query_exception_raises_db_query_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.side_effect = Exception("query error")
        with self.assertRaises(DBQueryException):
            db.query_master_by_doi(mock_app, "10.1234/abc")


# ---------------------------------------------------------------------------
# query_retry_files
# ---------------------------------------------------------------------------

class TestQueryRetryFiles(unittest.TestCase):

    def test_returns_filepaths(self):
        mock_app, mock_session = make_mock_app()
        expected = [("/path/a.xml",), ("/path/b.xml",)]
        mock_session.query.return_value.filter.return_value.all.return_value = expected
        result = db.query_retry_files(mock_app, "unmatched")
        self.assertEqual(result, expected)

    def test_exception_raises_db_query_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.side_effect = Exception("query error")
        with self.assertRaises(DBQueryException):
            db.query_retry_files(mock_app, "unmatched")


# ---------------------------------------------------------------------------
# write_block
# ---------------------------------------------------------------------------

class TestWriteBlock(unittest.TestCase):

    def test_success_calls_bulk_insert(self):
        mock_app, mock_session = make_mock_app()
        table = MagicMock()
        data = [{"key": "val1"}, {"key": "val2"}]
        db.write_block(mock_app, table, data)
        mock_session.bulk_insert_mappings.assert_called_once_with(table, data)
        mock_session.commit.assert_called_once()

    def test_exception_raises_db_write_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.bulk_insert_mappings.side_effect = Exception("insert failed")
        with self.assertRaises(DBWriteException):
            db.write_block(mock_app, MagicMock(), [])
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


# ---------------------------------------------------------------------------
# write_matched_record
# ---------------------------------------------------------------------------

class TestWriteMatchedRecord(unittest.TestCase):

    def test_insert_new_record_when_result_empty(self):
        # Empty result from query_master_by_doi → new row added
        mock_app, mock_session = make_mock_app()
        db.write_matched_record(mock_app, [], _make_matched_record())
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_update_existing_record_when_result_nonempty(self):
        # Non-empty result → update_master_by_doi called, not session.add
        mock_app, mock_session = make_mock_app()
        with patch("adscompstat.database.update_master_by_doi") as mock_update:
            db.write_matched_record(mock_app, [("existing_row",)], _make_matched_record())
            mock_update.assert_called_once()
        mock_session.add.assert_not_called()

    def test_exception_on_add_raises_db_write_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.add.side_effect = Exception("write error")
        with self.assertRaises(DBWriteException):
            db.write_matched_record(mock_app, [], _make_matched_record())
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


# ---------------------------------------------------------------------------
# write_completeness_summary
# ---------------------------------------------------------------------------

class TestWriteCompletenessSummary(unittest.TestCase):

    def _summary_data(self):
        return ["ApJ", "999", 100, 0.95, "[]", "[]"]

    def test_success_adds_and_commits(self):
        mock_app, mock_session = make_mock_app()
        db.write_completeness_summary(mock_app, self._summary_data())
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_exception_raises_db_write_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.add.side_effect = Exception("write failed")
        with self.assertRaises(DBWriteException):
            db.write_completeness_summary(mock_app, self._summary_data())
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


# ---------------------------------------------------------------------------
# update_master_by_doi
# ---------------------------------------------------------------------------

class TestUpdateMasterByDoi(unittest.TestCase):

    def test_success_calls_filter_update_commit(self):
        mock_app, mock_session = make_mock_app()
        update = {"master_doi": "10.1234/abc", "status": "Matched"}
        db.update_master_by_doi(mock_app, update)
        mock_session.query.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_exception_raises_db_write_exception(self):
        mock_app, mock_session = make_mock_app()
        mock_session.query.side_effect = Exception("update failed")
        with self.assertRaises(DBWriteException):
            db.update_master_by_doi(mock_app, {"master_doi": "10.1234/abc"})
        mock_session.rollback.assert_called()
        mock_session.flush.assert_called()


if __name__ == "__main__":
    unittest.main()
