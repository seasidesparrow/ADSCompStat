import os
import unittest

from adscompstat.match import CrossrefMatcher


class TestMatch(unittest.TestCase):
    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self.maxDiff = None

    def test__compare_bibstems(self):
        cm = CrossrefMatcher()
        cm.related_bibstems = [["Testa", "Testb"]]

        # test one, unrelated bibstems
        bibstem_a = "Foo.."
        bibstem_b = "Bar.."
        self.assertFalse(cm._compare_bibstems(bibstem_a, bibstem_b))

        # test two, related bibstems
        bibstem_a = "Testa"
        bibstem_b = "Testb"
        self.assertEqual(cm._compare_bibstems(bibstem_a, bibstem_b), "related")

        # test three, identical bibstems
        bibstem_a = "Testc"
        bibstem_b = "Testc"
        self.assertEqual(cm._compare_bibstems(bibstem_a, bibstem_b), "matched")

    def test__match_bibcode_permutations(self):
        cm = CrossrefMatcher()

        # test four, completely mismatched bibcodes
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "1900A&A...123..456X"
        correct_returnDict = {"match": "mismatch", "bibcode": "1900A&A...123..456X"}
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(test_returnDict, correct_returnDict)

        # test five, wrong author initial, year, volume, and page
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "2001ApJ...998..987Q"
        correct_returnDict = {
            "match": "partial",
            "bibcode": "2001ApJ...998..987Q",
            "errs": {"init": "Q", "page": ".987", "vol": ".998", "year": "2001"},
        }
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(test_returnDict, correct_returnDict)

        # test six, identical bibcodes
        # note, for completeness only -- in production this shouldn't happen
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "2000ApJ...999..999Z"
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        correct_returnDict = {"match": "partial", "errs": {}, "bibcode": "2000ApJ...999..999Z"}
        self.assertEqual(test_returnDict, correct_returnDict)

    def test_match(self):
        cm = CrossrefMatcher()

        # test seven, exact match exists in canonical for bibcode and DOI
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {"bibcode": "2000ApJ...999..999Z", "errs": {}, "match": "canonical"}
        self.assertEqual(test_returnDict, correct_returnDict)

        # test eight, DOI assigned to different but similar bibcode in classic,
        #     no canonical, alternate, or deleted matches
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..777Q", "2000ApJ...999..777Q", "canonical")]
        test_bibcodesFromBib = []
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {
            "bibcode": "2000ApJ...999..777Q",
            "errs": {"page": ".777", "init": "Q"},
            "match": "partial",
        }
        self.assertEqual(test_returnDict, correct_returnDict)

        # test nine, DOI assigned to totally different bibcode in classic,
        #     no canonical, alternate, or deleted matches
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("1900A&A...123..456X", "1900A&A...123..456X", "canonical")]
        test_bibcodesFromBib = []
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {"bibcode": "1900A&A...123..456X", "match": "mismatch"}
        self.assertEqual(test_returnDict, correct_returnDict)

        # test ten, neither bibcode nor DOI in classic
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = []
        test_bibcodesFromBib = []
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {
            "bibcode": None,
            "errs": {"DOI": "DOI not in classic"},
            "match": "unmatched",
        }
        self.assertEqual(test_returnDict, correct_returnDict)

        # test eleven, conflicting exact matches exist in canonical
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..777Q", "2000ApJ...999..777Q", "canonical")]
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {
            "bibcode": "2000ApJ...999..777Q",
            "errs": {"DOI": "DOI mismatched", "bibcode": "2000ApJ...999..999Z"},
            "match": "mismatch",
        }
        self.assertEqual(test_returnDict, correct_returnDict)

        # test twelve, bibcode exists in classic, DOI does not
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = []
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {
            "bibcode": "2000ApJ...999..999Z",
            "errs": {"DOI": "DOI not in classic"},
            "match": "canonical",
        }
        self.assertEqual(test_returnDict, correct_returnDict)
