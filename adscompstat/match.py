import adscompstat.utils as utils
from config import *

class CrossrefMatcher(object):

    def __init__(self):
        self.classicDoiBibDict = utils.load_classic_doi_bib_dict(CLASSIC_DOI_FILE)
        self.invertedBibDoiDict = utils.invert_doi_bib_dict(self.classicDoiBibDict)
        self.canonicalBibList = set(utils.load_classic_canonical_list(CLASSIC_CANONICAL))
        self.classicAllBibDict = utils.load_classic_noncanonical_bibs(CLASSIC_ALLBIBS)
        self.classicAltBibDict = utils.load_classic_noncanonical_bibs(CLASSIC_ALTBIBS)
        self.classicDelBibDict = utils.load_classic_noncanonical_bibs(CLASSIC_DELBIBS)


    def _get_doi_match(self, crossref_doi):
        return self.classicDoiBibDict.get(crossref_doi, None)

    def _match_bibcode_permutations(self, testBibcode, classicBibcode):
        try:
            returnDict = {}
            # check the year
            testYear = int(testBibcode[0:4])
            classicYear = int(classicBibcode[0:4])
            # check the qualifier letter
            testQual = testBibcode[13]
            classicQual = classicBibcode[13]
            # check the author init
            testInit = testBibcode[18]
            classicInit = classicBibcode[18]
            # rest of bibcode
            testRemainder = testBibcode[4:13] + testBibcode[14:18]
            classicRemainder = classicBibcode[4:13] + classicBibcode[14:18]
            if testRemainder == classicRemainder:
                returnDict['match'] = 'Partial'
                errs = {}
                returnDict['bibcode'] = classicBibcode
                if testYear != classicYear:
                    errs['year'] = classicYear
                if testQual != classicQual:
                    errs['qual'] = classicQual
                if testInit != classicInit:
                    errs['init'] = classicInit
                returnDict['errs'] = errs
            else:
                returnDict['match'] = 'Mismatch'
                returnDict['bibcode'] = classicBibcode
        except Exception as err:
            return
        return returnDict


    def _compare_bibcodes(self, testBibcode, classicBibcode):
        # use this if the Crossref DOI is also in classic and you
        # want to match the bibcode that's in classic
        try:
            returnDict = {}
            if testBibcode == classicBibcode:
                returnDict['match'] = 'Exact'
                returnDict['bibcode'] = classicBibcode
            else:
                altBibList = self.classicAltBibDict.get(classicBibcode, [])
                delBibList = self.classicDelBibDict.get(classicBibcode, [])
                allBibList = self.classicAllBibDict.get(classicBibcode, [])
                if testBibcode in altBibList:
                    returnDict['match'] = 'Alternate'
                    returnDict['bibcode'] = classicBibcode
                elif testBibcode in delBibList:
                    returnDict['match'] = 'Deleted'
                    returnDict['bibcode'] = classicBibcode
                elif testBibcode in allBibList:
                    returnDict['match'] = 'Other'
                    returnDict['bibcode'] = classicBibcode
                else:
                    returnDict = self._match_bibcode_permutations(testBibcode, classicBibcode)
            if returnDict:
                return returnDict
            else:
                return
        except Exception as err:
            return


    def match(self, xrefDOI, xrefBibcode):
        try:
            classicBibcode = self._get_doi_match(xrefDOI)
            if classicBibcode:
                result = self._compare_bibcodes(xrefBibcode, classicBibcode)
            else:
                result = dict()
                if xrefBibcode in self.canonicalBibList:
                    checkDoiList = self.invertedBibDoiDict.get(xrefBibcode, None)
                    if checkDoiList:
                        if xrefDOI not in checkDoiList:
                            result['match'] = 'Other'
                            result['bibcode'] = xrefBibcode
                            result['errs'] = {'DOI': 'Classic DOI is different.'}
                    else:
                        result['match'] = 'Other'
                        result['bibcode'] = xrefBibcode
                        result['errs'] = {'DOI': 'Not in classic.' }
                else:
                    result['match'] = 'Unmatched'
                    result['bibcode'] = None
                    result['errs'] = {'DOI': 'Not in classic.'}
            if result:
                return result
        except Exception as err:
            print('Error in match: %s' % err)
        return
