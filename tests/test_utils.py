import json
import os
import tempfile
import unittest

from adscompstat import utils
from adscompstat.exceptions import (
    CompletenessFractionException,
    JsonExportException,
    LoadIssnDataException,
    MissingFilenameException,
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self.maxDiff = None

    # ------------------------------------------------------------------
    # get_updateagent_logs
    # ------------------------------------------------------------------

    def test_get_updateagent_logs(self):
        logdir = "/nonexistent_path/"
        self.assertRaises(Exception, utils.get_updateagent_logs(logdir))

        logdir = "tests/stubdata/input/UpdateAgent/"
        test_infiles = utils.get_updateagent_logs(logdir)
        correct_infiles = ["tests/stubdata/input/UpdateAgent/10.3847:4879.out.2023-08-25"]
        self.assertEqual(test_infiles, correct_infiles)

    # ------------------------------------------------------------------
    # parse_pub_and_date_from_logs
    # ------------------------------------------------------------------

    def test_parse_pub_and_date_from_logs(self):
        test_infiles = ["tests/stubdata/input/UpdateAgent/10.3847:4879.out.2023-08-25"]
        (test_dates, test_pubdois) = utils.parse_pub_and_date_from_logs(test_infiles)
        correct_dates = ["2023-08-25"]
        correct_pubdois = ["10.3847"]
        self.assertEqual(test_dates, correct_dates)
        self.assertEqual(test_pubdois, correct_pubdois)

        test_infiles_fail = ["/nonexistent/path"]
        with self.assertRaises(Exception):
            utils.parse_pub_and_date_from_logs(test_infiles_fail)

    def test_parse_pub_and_date_from_logs_multiple_files(self):
        # Multiple files from different publishers/dates should be de-duped and sorted
        test_infiles = [
            "tests/stubdata/input/UpdateAgent/10.1234:0000.out.2023-01-10",
            "tests/stubdata/input/UpdateAgent/10.5678:0000.out.2023-03-01",
            "tests/stubdata/input/UpdateAgent/10.1234:0000.out.2023-01-10",  # duplicate
        ]
        (test_dates, test_pubdois) = utils.parse_pub_and_date_from_logs(test_infiles)
        self.assertEqual(test_dates, ["2023-01-10", "2023-03-01"])
        self.assertEqual(test_pubdois, ["10.1234", "10.5678"])

    # ------------------------------------------------------------------
    # read_updateagent_log
    # ------------------------------------------------------------------

    def test_read_updateagent_log(self):
        test_logfile = "tests/stubdata/input/UpdateAgent/10.3847:4879.out.2023-08-25"
        test_xmlfiles = utils.read_updateagent_log(test_logfile)
        correct_xmlfiles = [
            "doi/10.3847/./00/67/-0/04/9=/22/5=/2=/32//metadata.xml",
            "doi/10.3847/./00/67/-0/04/9=/22/6=/1=/3//metadata.xml",
            "doi/10.3847/./00/67/-0/04/9=/22/6=/1=/12//metadata.xml",
            "doi/10.3847/./00/67/-0/04/9=/22/7=/1=/8//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/aa/73/33//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/de/e5//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e7/7c//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e4/44//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/dd/e3//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e1/13//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e1/02//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e0/4a//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e6/16//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e1/e7//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/e4/c6//metadata.xml",
            "doi/10.3847/./15/38/-4/36/5=/ac/dd/06//metadata.xml",
        ]

        self.assertEqual(test_xmlfiles, correct_xmlfiles)

        test_logfile_fail = "/nonexistent/path"
        with self.assertRaises(Exception):
            utils.read_updateagent_log(test_logfile_fail)

    # ------------------------------------------------------------------
    # process_one_meta_xml
    # ------------------------------------------------------------------

    def test_process_one_meta_xml(self):
        test_infile = "tests/stubdata/input/test_metadata.xml"
        test_record = utils.process_one_meta_xml(test_infile)

        test_doi = test_record.get("master_doi", None)
        correct_doi = "10.3847/0004-637X/816/1/36"
        self.assertEqual(test_doi, correct_doi)

        test_filepath = test_record.get("harvest_filepath", None)
        correct_filepath = "tests/stubdata/input/test_metadata.xml"
        self.assertEqual(test_filepath, correct_filepath)

        test_issns = test_record.get("issns", None)
        correct_issns = {"electronic": "1538-4357"}
        self.assertEqual(test_issns, correct_issns)

    def test_process_one_meta_xml_nonexistent_file(self):
        # A missing file should return an error dict rather than raise
        result = utils.process_one_meta_xml("/nonexistent/path/metadata.xml")
        self.assertIn("status", result)
        self.assertIn("error", result["status"])
        self.assertEqual(result.get("harvest_filepath"), "/nonexistent/path/metadata.xml")

    # ------------------------------------------------------------------
    # load_classic_doi_bib_map
    # ------------------------------------------------------------------

    def test_load_classic_doi_bib_map(self):
        test_infile = "tests/stubdata/input/doi_links"
        test_result = utils.load_classic_doi_bib_map(test_infile)
        correct_result = [
            {"doi": "10.3847/1538-4357/aca76b", "identifier": "2023ApJ...942....1C"},
            {"doi": "10.3847/1538-4357/aca541", "identifier": "2023ApJ...942....2T"},
            {"doi": "10.3847/1538-4357/aca52c", "identifier": "2023ApJ...942....3Y"},
        ]
        self.assertEqual(test_result, correct_result)

    def test_load_classic_doi_bib_map_duplicate_doi(self):
        # Duplicate DOIs should be silently de-duplicated (first occurrence kept)
        content = (
            "2023ApJ...942....1C\t10.3847/1538-4357/aca76b\n"
            "2023ApJ...942....9Z\t10.3847/1538-4357/aca76b\n"  # same DOI, different bibcode
            "2023ApJ...942....2T\t10.3847/1538-4357/aca541\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(content)
            tmpname = f.name
        try:
            result = utils.load_classic_doi_bib_map(tmpname)
            dois = [r["doi"] for r in result]
            self.assertEqual(len(dois), len(set(dois)), "Duplicate DOIs were not de-duped")
            self.assertEqual(result[0]["identifier"], "2023ApJ...942....1C")
        finally:
            os.unlink(tmpname)

    def test_load_classic_doi_bib_map_bad_line(self):
        # Lines that cannot be split into (bibcode, doi) should be skipped
        content = (
            "2023ApJ...942....1C\t10.3847/1538-4357/aca76b\n"
            "THIS_LINE_HAS_NO_TAB\n"
            "2023ApJ...942....2T\t10.3847/1538-4357/aca541\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(content)
            tmpname = f.name
        try:
            result = utils.load_classic_doi_bib_map(tmpname)
            self.assertEqual(len(result), 2)
        finally:
            os.unlink(tmpname)

    # ------------------------------------------------------------------
    # load_journalsdb_issn_bibstem_list
    # ------------------------------------------------------------------

    def test_load_journalsdb_issn_bibstem_list(self):
        test_infile = "tests/stubdata/input/issn_bibstems"
        test_result = utils.load_journalsdb_issn_bibstem_list(test_infile)
        correct_result = [
            {"issn": "0004-637X", "bibstem": "ApJ", "issn_type": "ISSN_print"},
            {"issn": "1538-4357", "bibstem": "ApJ", "issn_type": "ISSN_electronic"},
            {"issn": "0556-2821", "bibstem": "PhRvD", "issn_type": "ISSN_print"},
            {"issn": "2470-0029", "bibstem": "PhRvD", "issn_type": "ISSN_electronic"},
        ]
        self.assertEqual(test_result, correct_result)

    def test_load_journalsdb_issn_bibstem_list_duplicate_issn(self):
        # Duplicate ISSNs should be de-duped (first occurrence kept)
        content = (
            "ApJ\tISSN_print\t0004-637X\n"
            "ApJS\tISSN_print\t0004-637X\n"  # duplicate ISSN
            "ApJ\tISSN_electronic\t1538-4357\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(content)
            tmpname = f.name
        try:
            result = utils.load_journalsdb_issn_bibstem_list(tmpname)
            issns = [r["issn"] for r in result]
            self.assertEqual(len(issns), len(set(issns)), "Duplicate ISSNs were not de-duped")
            self.assertEqual(result[0]["bibstem"], "ApJ")
        finally:
            os.unlink(tmpname)

    def test_load_journalsdb_issn_bibstem_list_bad_file(self):
        # A file with malformed lines should raise LoadIssnDataException
        content = "ONLY_ONE_COLUMN\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(content)
            tmpname = f.name
        try:
            with self.assertRaises(LoadIssnDataException):
                utils.load_journalsdb_issn_bibstem_list(tmpname)
        finally:
            os.unlink(tmpname)

    # ------------------------------------------------------------------
    # load_classic_canonical_list
    # ------------------------------------------------------------------

    def test_load_classic_canonical_list(self):
        test_infile = "tests/stubdata/input/canonical_list"
        test_result = utils.load_classic_canonical_list(test_infile)
        correct_result = [
            "2020ApJ...777...13A",
            "2020ApJ...777...14Q",
            "2020ApJ...777...15A",
            "2020ApJ...777...16A",
            "2020ApJ...777...18A",
        ]
        self.assertEqual(test_result, correct_result)

    def test_load_classic_canonical_list_bad_lines(self):
        # Lines that are not exactly 19 chars should be silently skipped
        content = (
            "2020ApJ...777...13A\n"   # valid (19 chars)
            "TOOSHORT\n"              # invalid
            "2020ApJ...777...14Q\n"   # valid
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(content)
            tmpname = f.name
        try:
            result = utils.load_classic_canonical_list(tmpname)
            self.assertEqual(result, ["2020ApJ...777...13A", "2020ApJ...777...14Q"])
        finally:
            os.unlink(tmpname)

    # ------------------------------------------------------------------
    # load_classic_noncanonical_bibs
    # ------------------------------------------------------------------

    def test_load_classic_noncanonical_bibs(self):
        test_infile = "tests/stubdata/input/deleted_list"
        test_result = utils.load_classic_noncanonical_bibs(test_infile)
        correct_result = {
            "2013xyzp.conf..208F": "none",
            "2019ApJ...777...18A": "2020ApJ...777...18A",
        }
        self.assertEqual(test_result, correct_result)

    # ------------------------------------------------------------------
    # merge_bibcode_lists
    # ------------------------------------------------------------------

    def test_merge_bibcode_lists(self):
        test_canonical_file = "tests/stubdata/input/canonical_list"
        test_alternate_file = "tests/stubdata/input/alternate_list"
        test_deleted_file = "tests/stubdata/input/deleted_list"
        test_allbib_file = "tests/stubdata/input/all_list"
        test_result = utils.merge_bibcode_lists(
            test_canonical_file, test_alternate_file, test_deleted_file, test_allbib_file
        )
        correct_result = [
            {
                "canonical_id": "2020ApJ...777...13A",
                "identifier": "2020ApJ...777...13A",
                "idtype": "canonical",
            },
            {
                "canonical_id": "2020ApJ...777...14Q",
                "identifier": "2020ApJ...777...14Q",
                "idtype": "canonical",
            },
            {
                "canonical_id": "2020ApJ...777...15A",
                "identifier": "2020ApJ...777...15A",
                "idtype": "canonical",
            },
            {
                "canonical_id": "2020ApJ...777...16A",
                "identifier": "2020ApJ...777...16A",
                "idtype": "canonical",
            },
            {
                "canonical_id": "2020ApJ...777...18A",
                "identifier": "2020ApJ...777...18A",
                "idtype": "canonical",
            },
            {
                "canonical_id": "2020ApJ...777...14Q",
                "identifier": "2020ApJ...777...14P",
                "idtype": "alternate",
            },
            {"canonical_id": "none", "identifier": "2013xyzp.conf..208F", "idtype": "deleted"},
            {
                "canonical_id": "2020ApJ...777...18A",
                "identifier": "2019ApJ...777...18A",
                "idtype": "deleted",
            },
        ]
        self.assertEqual(test_result, correct_result)

    # ------------------------------------------------------------------
    # get_completeness_fraction
    # ------------------------------------------------------------------

    def test_get_completeness_fraction(self):
        test_summary = [
            {"year": "2019", "status": "Matched", "matchtype": "canonical", "count": 150},
            {"year": "2019", "status": "Matched", "matchtype": "deleted", "count": 3},
            {"year": "2019", "status": "Matched", "matchtype": "mismatch", "count": 4},
            {"year": "2019", "status": "Unmatched", "matchtype": "unmatched", "count": 13},
        ]
        test_fraction = utils.get_completeness_fraction(test_summary)
        test_result = (
            test_fraction.get("volumeIndexable", 0),
            test_fraction.get("volumeCompleteness", 0),
        )
        correct_result = (170, 0.9)
        self.assertEqual(test_result, correct_result)

    def test_get_completeness_fraction_returns_by_year(self):
        # The bundle should include a by_year list with one entry per year
        test_summary = [
            {"year": "2019", "status": "Matched", "matchtype": "canonical", "count": 100},
            {"year": "2019", "status": "Unmatched", "matchtype": "unmatched", "count": 10},
        ]
        result = utils.get_completeness_fraction(test_summary)
        by_year = result.get("by_year", [])
        self.assertEqual(len(by_year), 1)
        entry = by_year[0]
        self.assertEqual(entry["year"], "2019")
        self.assertEqual(entry["ADS_records"], 100)
        self.assertEqual(entry["Crossref_records"], 110)

    def test_get_completeness_fraction_multiple_years(self):
        test_summary = [
            {"year": "2018", "status": "Matched", "matchtype": "canonical", "count": 100},
            {"year": "2018", "status": "Unmatched", "matchtype": "unmatched", "count": 10},
            {"year": "2019", "status": "Matched", "matchtype": "canonical", "count": 150},
            {"year": "2019", "status": "Unmatched", "matchtype": "unmatched", "count": 20},
        ]
        result = utils.get_completeness_fraction(test_summary)
        # 2018: 100 matched, 110 total; 2019: 150 matched, 170 total
        self.assertEqual(result["volumeMatched"], 250)
        self.assertEqual(result["volumeIndexable"], 280)
        self.assertAlmostEqual(result["volumeCompleteness"], 250 / 280)
        by_year = {e["year"]: e for e in result["by_year"]}
        self.assertIn("2018", by_year)
        self.assertIn("2019", by_year)

    def test_get_completeness_fraction_year_as_integer(self):
        # year values coming from DB may be integers; code converts with str()
        test_summary = [
            {"year": 2020, "status": "Matched", "matchtype": "canonical", "count": 80},
            {"year": 2020, "status": "Unmatched", "matchtype": "unmatched", "count": 20},
        ]
        result = utils.get_completeness_fraction(test_summary)
        self.assertEqual(result["volumeMatched"], 80)
        self.assertEqual(result["volumeIndexable"], 100)
        self.assertAlmostEqual(result["volumeCompleteness"], 0.8)

    def test_get_completeness_fraction_empty_input(self):
        # Zero indexable records → ZeroDivisionError → CompletenessFractionException
        with self.assertRaises(CompletenessFractionException):
            utils.get_completeness_fraction([])

    def test_get_completeness_fraction_noindex_only(self):
        # Records with status NoIndex do not count toward indexable total,
        # so volumeIndexable stays 0 → ZeroDivisionError → CompletenessFractionException
        test_summary = [
            {"year": "2019", "status": "NoIndex", "matchtype": "other", "count": 50},
        ]
        with self.assertRaises(CompletenessFractionException):
            utils.get_completeness_fraction(test_summary)

    def test_get_completeness_fraction_partial_and_alternate(self):
        # "partial" and "alternate" matchtypes count as matched
        test_summary = [
            {"year": "2021", "status": "Matched", "matchtype": "partial", "count": 40},
            {"year": "2021", "status": "Matched", "matchtype": "alternate", "count": 10},
            {"year": "2021", "status": "Unmatched", "matchtype": "unmatched", "count": 50},
        ]
        result = utils.get_completeness_fraction(test_summary)
        self.assertEqual(result["volumeMatched"], 50)
        self.assertEqual(result["volumeIndexable"], 100)
        self.assertAlmostEqual(result["volumeCompleteness"], 0.5)

    # ------------------------------------------------------------------
    # export_completeness_data
    # ------------------------------------------------------------------

    def test_export_completeness_data_success(self):
        data = [{"bibstem": "ApJ", "completeness": 0.95}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            tmpname = f.name
        try:
            utils.export_completeness_data(data, tmpname)
            with open(tmpname, "r") as f:
                loaded = json.loads(f.read())
            self.assertEqual(loaded, data)
        finally:
            os.unlink(tmpname)

    def test_export_completeness_data_no_filename(self):
        with self.assertRaises(MissingFilenameException):
            utils.export_completeness_data([], None)

    def test_export_completeness_data_bad_path(self):
        with self.assertRaises(JsonExportException):
            utils.export_completeness_data([], "/nonexistent_dir/output.json")


if __name__ == "__main__":
    unittest.main()
