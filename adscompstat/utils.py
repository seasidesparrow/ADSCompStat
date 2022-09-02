import re
from bs4 import BeautifulSoup
from adscompstat.exceptions import *
from adsingestp.parsers.crossref import CrossrefParser
from adsingestp.parsers.base import BaseBeautifulSoupParser
from glob import glob

re_issn = re.compile(r"^\d{4}-?\d{3}[0-9X]$")


def get_updateagent_logs(logdir):
    try:
        return glob(logdir+'*.out.*')
    except Exception as err:
        raise NoHarvestLogsException(err)


def parse_pub_and_date_from_logs(infiles):
    try:
        dates = []
        pubdois = []
        for f in infiles:
            fstrip = f.split('/')[-1]
            (doi_base,harvest_date) = fstrip.split('.out.')
            dates.append(harvest_date)
            doi_base = doi_base.split(':')[0]
            pubdois.append(doi_base)
        dates = list(set(dates))
        dates.sort()
        pubdois = list(set(pubdois))
        pubdois.sort()
        return dates, pubdois
    except Exception as err:
        raise ParseLogsException(err)


def read_updateagent_log(logfile):
    xmlfiles = []
    try:
        with open(logfile, 'r') as fl:
            for l in fl.readlines():
                (filename, harvest_time) = l.strip().split('\t')
                # xmlfiles.append({'filename': filename, 'harvest_time': harvest_time})
                xmlfiles.append(filename)
    except Exception as err:
        raise ReadLogException(err)
    else:
        return xmlfiles


def parse_one_meta_xml(filename):
    try:
        record = dict()
        with open(filename,'r') as fx:
            data = fx.read()
            try:
                parser = CrossrefParser()
                record = parser.parse(data)
            except Exception as err:
                raise CrossRefParseException(err)
        return record
    except Exception as err:
        raise ParseMetaXMLException(err)


def simple_parse_one_meta_xml(filename):
    try:
        with open(filename,'r') as fx:
            data = fx.read()
            try:
                parser = BaseBeautifulSoupParser()
                record = parser.bsstrtodict(data)
                doi = record.find("doi").get_text()
                issn_all = record.find_all("issn")
                issns = []
                for i in issn_all:
                    if i.get_text() and re_issn.match(i.get_text()):
                        if i.has_attr("media_type"):
                            issns.append((i["media_type"], i.get_text()))
                        else:
                            issns.append(("print", i.get_text()))
            except Exception as err:
                raise BaseParseException(err)
        return (doi, issns)
    except Exception as err:
        raise ParseMetaXMLException(err)


def load_classic_doi_bib_dict(infile):
    # Classic: DOI-bibcode mapping 
    classic_bib_doi_dict = dict()
    try:
        with open(infile, 'r') as fa:
            for l in fa.readlines():
                try:
                    (bibcode, doi) = l.strip().split('\t')
                    classic_bib_doi_dict[doi] = bibcode
                except Exception as err:
                    # logger.debug("bad line in %s: %s" % (infile, l.strip()))
                    pass
    except Exception as err:
        raise LoadClassicDataException("Unable to load classic dois and bibcodes! %s" % err)
    return classic_bib_doi_dict


def invert_doi_bib_dict(input_dict):
    inverted_dict = dict()
    if input_dict:
        for k, v in input_dict.items():
            try:
                newkey = v
                newval = k
                if inverted_dict.get(newkey, None):
                    inverted_dict[newkey].append(newval)
                else:
                    inverted_dict[newkey] = [newval]
            except Exception as err:
                # logger.debug("problem in invert_doi_bib_dict (%s, %s): %s" % (k, v, err))
                pass
    return inverted_dict


def load_classic_canonical_list(infile):
    canonicalList = list()
    try:
        with open(infile, 'r') as fc:
            for l in fc.readlines():
                bibcode = l.strip()
                if len(bibcode) == 19:
                    canonicalList.append(bibcode)
                else:
                    # logger.debug("bad line in %s: %s" % (infile, l.strip()))
                    pass
    except Exception as err:
        raise LoadClassicDataException("Unable to load canonical bibcodes list! %s" % err)
    return canonicalList


def load_classic_noncanonical_bibs(bibfile):
    try:
        noncbibcodes = dict()
        with open(bibfile, 'r') as fi:
            for l in fi.readlines():
                try:
                    (noncbib, canonical) = l.strip().split()
                except Exception:
                    # logger.debug("singleton bibcode in %s: %s" % (infile, l.strip()))
                    pass
                else:
                    if noncbibcodes.get(canonical, None):
                        noncbibcodes[canonical].append(noncbib)
                    else:
                        noncbibcodes[canonical] = [noncbib]
    except Exception as err:
        raise LoadClassicDataException("Unable to load classic alt/del bibcodes! %s" % err)
    else:
        return noncbibcodes
