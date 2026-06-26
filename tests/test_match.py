import os
import unittest

from adscompstat.match import CrossrefMatcher


class TestMatch(unittest.TestCase):
    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")
        self.maxDiff = None

    # ------------------------------------------------------------------
    # _compare_bibstems
    # ------------------------------------------------------------------

    def test__compare_bibstems(self):
        cm = CrossrefMatcher()
        cm.related_bibstems = [["Testa", "Testb"]]

        # unrelated bibstems → falsy
        bibstem_a = "Foo.."
        bibstem_b = "Bar.."
        self.assertFalse(cm._compare_bibstems(bibstem_a, bibstem_b))

        # related bibstems → "related"
        bibstem_a = "Testa"
        bibstem_b = "Testb"
        self.assertEqual(cm._compare_bibstems(bibstem_a, bibstem_b), "related")

        # identical bibstems → "matched"
        bibstem_a = "Testc"
        bibstem_b = "Testc"
        self.assertEqual(cm._compare_bibstems(bibstem_a, bibstem_b), "matched")

    def test__compare_bibstems_one_in_relation_other_not(self):
        # Only one bibstem is in the relation set → not related
        cm = CrossrefMatcher(related_bibstems=[["Testa", "Testb"]])
        self.assertFalse(cm._compare_bibstems("Testa", "Testc"))

    def test__compare_bibstems_multiple_relation_groups(self):
        # Bibstems from different groups should not match each other
        cm = CrossrefMatcher(related_bibstems=[["JGR..", "JGRD."], ["ApJ..", "ApJS."]])
        self.assertEqual(cm._compare_bibstems("JGR..", "JGRD."), "related")
        self.assertEqual(cm._compare_bibstems("ApJ..", "ApJS."), "related")
        self.assertFalse(cm._compare_bibstems("JGR..", "ApJ.."))

    # ------------------------------------------------------------------
    # _match_bibcode_permutations
    # ------------------------------------------------------------------

    def test__match_bibcode_permutations(self):
        cm = CrossrefMatcher()

        # completely mismatched bibcodes → mismatch
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "1900A&A...123..456X"
        correct_returnDict = {"match": "mismatch", "bibcode": "1900A&A...123..456X"}
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(test_returnDict, correct_returnDict)

        # wrong author initial, year, volume, and page → partial with all four errors
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "2001ApJ...998..987Q"
        correct_returnDict = {
            "match": "partial",
            "bibcode": "2001ApJ...998..987Q",
            "errs": {"init": "Q", "page": ".987", "vol": ".998", "year": "2001"},
        }
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(test_returnDict, correct_returnDict)

        # identical bibcodes → partial with empty errs
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "2000ApJ...999..999Z"
        test_returnDict = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        correct_returnDict = {"match": "partial", "errs": {}, "bibcode": "2000ApJ...999..999Z"}
        self.assertEqual(test_returnDict, correct_returnDict)

    def test__match_bibcode_permutations_none_test_bibcode(self):
        # No test bibcode, classic exists → "failed"
        cm = CrossrefMatcher()
        result = cm._match_bibcode_permutations(None, "2000ApJ...999..999Z")
        self.assertEqual(result, {"match": "failed", "bibcode": "2000ApJ...999..999Z"})

    def test__match_bibcode_permutations_none_classic_bibcode(self):
        # Test bibcode exists, no classic → "unmatched"
        cm = CrossrefMatcher()
        result = cm._match_bibcode_permutations("2000ApJ...999..999Z", None)
        self.assertEqual(result, {"match": "unmatched", "bibcode": None})

    def test__match_bibcode_permutations_both_none(self):
        # Both None → empty dict (no meaningful comparison possible)
        cm = CrossrefMatcher()
        result = cm._match_bibcode_permutations(None, None)
        self.assertEqual(result, {})

    def test__match_bibcode_permutations_related_bibstem(self):
        # Bibstems are different but related → partial with bibstem in errs
        cm = CrossrefMatcher(related_bibstems=[["ApJ..", "ApJS."]])
        bibcode_a = "2000ApJ...999..999Z"  # bibstem = "ApJ.."
        bibcode_b = "2000ApJS..999..999Z"  # bibstem = "ApJS."
        result = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(result["match"], "partial")
        self.assertEqual(result["bibcode"], bibcode_b)
        self.assertIn("bibstem", result["errs"])
        self.assertEqual(result["errs"]["bibstem"], "related")

    def test__match_bibcode_permutations_qualifier_diff(self):
        # Only qualifier differs → partial with qual in errs
        cm = CrossrefMatcher()
        bibcode_a = "2000ApJ...999..999Z"  # qual = '.'
        bibcode_b = "2000ApJ...999Q.999Z"  # qual = 'Q'
        result = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(result["match"], "partial")
        self.assertEqual(result["bibcode"], bibcode_b)
        self.assertIn("qual", result["errs"])
        self.assertEqual(result["errs"]["qual"], "Q")
        # No other errors expected
        self.assertNotIn("year", result["errs"])
        self.assertNotIn("vol", result["errs"])
        self.assertNotIn("page", result["errs"])
        self.assertNotIn("init", result["errs"])

    def test__match_bibcode_permutations_init_diff_only(self):
        # Only author initial differs
        cm = CrossrefMatcher()
        bibcode_a = "2000ApJ...999..999Z"
        bibcode_b = "2000ApJ...999..999A"
        result = cm._match_bibcode_permutations(bibcode_a, bibcode_b)
        self.assertEqual(result["match"], "partial")
        self.assertEqual(result["errs"], {"init": "A"})

    # ------------------------------------------------------------------
    # match
    # ------------------------------------------------------------------

    def test_match(self):
        cm = CrossrefMatcher()

        # exact match exists in canonical for bibcode and DOI
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {"bibcode": "2000ApJ...999..999Z", "errs": {}, "match": "canonical"}
        self.assertEqual(test_returnDict, correct_returnDict)

        # DOI assigned to different but similar bibcode in classic, no bib match
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

        # DOI assigned to totally different bibcode in classic, no bib match → mismatch
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("1900A&A...123..456X", "1900A&A...123..456X", "canonical")]
        test_bibcodesFromBib = []
        test_returnDict = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        correct_returnDict = {"bibcode": "1900A&A...123..456X", "match": "mismatch"}
        self.assertEqual(test_returnDict, correct_returnDict)

        # neither bibcode nor DOI in classic → unmatched
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

        # conflicting exact matches in canonical → mismatch
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

        # bibcode exists in classic, DOI does not
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

    def test_match_none_doi_matches(self):
        # None for classicDoiMatches should behave identically to an empty list
        cm = CrossrefMatcher()
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "canonical")]

        result_none = cm.match(test_input_bibcode, None, test_bibcodesFromBib)
        result_empty = cm.match(test_input_bibcode, [], test_bibcodesFromBib)
        self.assertEqual(result_none, result_empty)

    def test_match_alternate_bibcode_type(self):
        # DOI maps to an alternate bibcode in classic
        cm = CrossrefMatcher()
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "alternate")]
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "alternate")]
        result = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        self.assertEqual(result["match"], "alternate")
        self.assertEqual(result["bibcode"], "2000ApJ...999..999Z")

    def test_match_deleted_bibcode_type(self):
        # DOI maps to a deleted bibcode in classic
        cm = CrossrefMatcher()
        test_input_bibcode = "2000ApJ...999..999Z"
        test_bibcodesFromDoi = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "deleted")]
        test_bibcodesFromBib = [("2000ApJ...999..999Z", "2000ApJ...999..999Z", "deleted")]
        result = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        self.assertEqual(result["match"], "deleted")

    def test_match_with_related_bibstems(self):
        # CrossrefMatcher initialized with related bibstems; DOI resolves to a
        # related-bibstem bibcode → partial with bibstem in errs
        cm = CrossrefMatcher(related_bibstems=[["JGR..", "JGRD."]])
        test_input_bibcode = "2000JGR...999..999Z"  # bibstem JGR..
        test_bibcodesFromDoi = [("2000JGRD..999..999Z", "2000JGRD..999..999Z", "canonical")]
        test_bibcodesFromBib = []
        result = cm.match(test_input_bibcode, test_bibcodesFromDoi, test_bibcodesFromBib)
        self.assertEqual(result["match"], "partial")
        self.assertIn("bibstem", result.get("errs", {}))


if __name__ == "__main__":
    unittest.main()
