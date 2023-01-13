import adscompstat.utils as utils
from config import *

class CrossrefMatcher(object):

    def __init__(self):
        pass

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

            if testBibstem == classicBibstem:
                returnDict['match'] = 'partial'
                errs = {}
                returnDict['bibcode'] = classicBibcode
                if testYear != classicYear:
                    errs['year'] = classicYear
                if testQual != classicQual:
                    errs['qual'] = classicQual
                if testInit != classicInit:
                    errs['init'] = classicInit
                if testVol != classicVol:
                    try:
                        if int(testVol) != int(classicVol):
                            errs['vol'] = classicVol
                    except Exception as err:
                        errs['vol'] = classicVol
                if testPage != classicPage:
                    try:
                        if int(testPage) != int(classicPage):
                            errs['page'] = classicPage
                    except Exception as err:
                        errs['page'] = classicPage
                returnDict['errs'] = errs
            else:
                returnDict['match'] = 'mismatch'
                returnDict['bibcode'] = classicBibcode
        except Exception as err:
            return {'match': 'mismatch', 'bibcode': classicBibcode}
        return returnDict


    def match(self, xrefBibcode, classicDoiMatches, classicBibMatches):
        result = {}
        try:
            # first, see if the generated bibcode is in classic
            resultBib = {}
            for match in classicBibMatches:
                if xrefBibcode == match[0]:
                    resultBib['match'] = match[2]
                    resultBib['bibcode'] = match[1]
                    resultBib['errs'] = {}
            resultDoi = {}
            if not classicDoiMatches:
                resultDoi['match'] = 'unmatched'
                resultDoi['bibcode'] = None
                resultDoi['errs'] = {'DOI': 'Not in classic.'}
            else:
                for match in classicDoiMatches:
                    if not resultDoi.get('bibcode', None):
                        if xrefBibcode == match[0]:
                            resultDoi['match'] = match[2]
                            resultDoi['bibcode'] = match[1]
                            resultDoi['errs'] = {}
                        else:
                            resultDoi = self._match_bibcode_permutations(xrefBibcode, match[0])
            if resultBib:
                if resultDoi.get('errs', None):
                    resultBib['errs'] = resultDoi.get('errs')
                result = resultBib
            elif resultDoi:
                result = resultDoi
        except Exception as err:
            logger.warning('Error matching Crossref-generated bibcode %s: %s' % (xrefBibcode, err))
        return result
