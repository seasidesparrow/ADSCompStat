import os
import unittest

from adscompstat import utils


class TestUtils(unittest.TestCase):
    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self.maxDiff = None

    def test_get_updateagent_logs(self):
        # test one, UpdateAgent log directory doesn"t exist
        logdir = "/nonexistent_path/"
        self.assertRaises(Exception, utils.get_updateagent_logs(logdir))

        # test two, UpdateAgent log directory exists and has file(s)
        logdir = "tests/stubdata/input/UpdateAgent/"
        test_infiles = utils.get_updateagent_logs(logdir)
        correct_infiles = ["tests/stubdata/input/UpdateAgent/10.3847:4879.out.2023-08-25"]
        self.assertEqual(test_infiles, correct_infiles)

    def test_parse_pub_and_date_from_logs(self):
        # test three, parse logfiles
        test_infiles = ["tests/stubdata/input/UpdateAgent/10.3847:4879.out.2023-08-25"]
        (test_dates, test_pubdois) = utils.parse_pub_and_date_from_logs(test_infiles)
        correct_dates = ["2023-08-25"]
        correct_pubdois = ["10.3847"]
        self.assertEqual(test_dates, correct_dates)
        self.assertEqual(test_pubdois, correct_pubdois)

    def test_read_updateagent_log(self):
        # test four, read an updateagent_log
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

    def test_process_one_meta_xml(self):
        # test six, process a crossref xml file
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

    def test_load_classic_doi_bib_map(self):
        test_infile = "tests/stubdata/input/doi_links"
        test_result = utils.load_classic_doi_bib_map(test_infile)
        correct_result = [
            {"doi": "10.3847/1538-4357/aca76b", "identifier": "2023ApJ...942....1C"},
            {"doi": "10.3847/1538-4357/aca541", "identifier": "2023ApJ...942....2T"},
            {"doi": "10.3847/1538-4357/aca52c", "identifier": "2023ApJ...942....3Y"},
        ]
        self.assertEqual(test_result, correct_result)

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

    def test_load_classic_noncanonical_bibs(self):
        test_infile = "tests/stubdata/input/deleted_list"
        test_result = utils.load_classic_noncanonical_bibs(test_infile)
        correct_result = {
            "2013xyzp.conf..208F": "none",
            "2019ApJ...777...18A": "2020ApJ...777...18A",
        }
        self.assertEqual(test_result, correct_result)

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

    def test_get_completeness_fraction(self):
        test_summary = [
            {"status": "Matched", "matchtype": "canonical", "count": 150},
            {"status": "Matched", "matchtype": "deleted", "count": 3},
            {"status": "Matched", "matchtype": "mismatch", "count": 4},
            {"status": "Unmatched", "matchtype": "unmatched", "count": 13},
        ]
        test_result = utils.get_completeness_fraction(test_summary)
        correct_result = (170, 0.9)
        self.assertEqual(test_result, correct_result)
