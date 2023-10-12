import os

from adsputils import load_config, setup_logging

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
conf = load_config(proj_home=proj_home)

logger = setup_logging(
    "completeness-statistics-pipeline",
    proj_home=proj_home,
    level=conf.get("LOGGING_LEVEL", "INFO"),
    attach_stdout=conf.get("LOG_STDOUT", False),
)


class CrossrefMatcher(object):
    def __init__(self, related_bibstems=[]):
        self.related_bibstems = related_bibstems

    def _compare_bibstems(self, testBibstem, classicBibstem):
        status = None
        try:
            if testBibstem == classicBibstem:
                status = "matched"
            else:
                for relation in self.related_bibstems:
                    if testBibstem in relation and classicBibstem in relation:
                        status = "related"
                        break
        except Exception as err:
            logger.debug("Exception in compare_bibstems: %s" % err)
        return status

    def _match_bibcode_permutations(self, testBibcode, classicBibcode):
        try:
            returnDict = {}
            # check the year
            testYear = testBibcode[0:4]
            classicYear = classicBibcode[0:4]
            # check the bibstem
            testBibstem = testBibcode[4:9]
            classicBibstem = classicBibcode[4:9]
            # check the base volume
            testVol = testBibcode[9:13]
            classicVol = classicBibcode[9:13]
            # check the qualifier letter
            testQual = testBibcode[13]
            classicQual = classicBibcode[13]
            # check the base page
            testPage = testBibcode[14:18]
            classicPage = classicBibcode[14:18]
            # check the author init
            testInit = testBibcode[18]
            classicInit = classicBibcode[18]

            stem = self._compare_bibstems(testBibstem, classicBibstem)
            if stem:
                returnDict["match"] = "partial"
                errs = {}
                returnDict["bibcode"] = classicBibcode
                if stem == "related":
                    errs["bibstem"] = stem
                if testYear != classicYear:
                    errs["year"] = classicYear
                if testQual != classicQual:
                    errs["qual"] = classicQual
                if testInit != classicInit:
                    errs["init"] = classicInit
                if testVol != classicVol:
                    try:
                        if int(testVol) != int(classicVol):
                            errs["vol"] = classicVol
                    except Exception as err:
                        logger.debug("errs[vol]: %s" % err)
                        errs["vol"] = classicVol
                if testPage != classicPage:
                    try:
                        if int(testPage) != int(classicPage):
                            errs["page"] = classicPage
                    except Exception as err:
                        logger.debug("errs[page]: %s" % err)
                        errs["page"] = classicPage
                returnDict["errs"] = errs
            else:
                returnDict["match"] = "mismatch"
                returnDict["bibcode"] = classicBibcode
        except Exception as err:
            logger.debug("Problem checking bibcode permutations: %s" % err)
            return {"match": "mismatch", "bibcode": classicBibcode}
        return returnDict

    def match(self, xrefBibcode, classicDoiMatches, classicBibMatches):
        result = {}
        try:
            # first, see if the generated bibcode is in classic
            resultBib = {}
            for match in classicBibMatches:
                if xrefBibcode == match[0]:
                    resultBib["match"] = match[2]
                    resultBib["bibcode"] = match[1]
                    resultBib["errs"] = {}
            resultDoi = {}
            if classicDoiMatches:
                for match in classicDoiMatches:
                    if not resultDoi.get("bibcode", None):
                        if xrefBibcode == match[0]:
                            resultDoi["match"] = match[2]
                            resultDoi["bibcode"] = match[1]
                            resultDoi["errs"] = {}
                        else:
                            resultDoi = self._match_bibcode_permutations(xrefBibcode, match[0])
            if resultDoi:
                if resultBib:
                    if resultBib["bibcode"] == resultDoi["bibcode"]:
                        resultDoi["match"] = resultBib["match"]
                        result = resultDoi
                    else:
                        result = {
                            "match": "mismatch",
                            "bibcode": resultDoi["bibcode"],
                            "errs": {"DOI": "DOI mismatched", "bibcode": resultBib["bibcode"]},
                        }
                else:
                    result = resultDoi
            elif resultBib:
                result = resultBib
                result["errs"]["DOI"] = "DOI not in classic"
            else:
                result["match"] = "unmatched"
                result["bibcode"] = None
                result["errs"] = {"DOI": "DOI not in classic"}
        except Exception as err:
            logger.warning("Error matching Crossref-generated bibcode %s: %s" % (xrefBibcode, err))
        return result
