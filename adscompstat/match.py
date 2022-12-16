import adscompstat.utils as utils
from config import *

class CrossrefMatcher(object):

    def __init__(self):
        pass

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
                returnDict['match'] = 'partial'
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
                returnDict['match'] = 'mismatch'
                returnDict['bibcode'] = classicBibcode
        except Exception as err:
            return {}
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
