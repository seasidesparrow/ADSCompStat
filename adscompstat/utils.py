import json
import os
import re
from bs4 import BeautifulSoup
from adscompstat.exceptions import *
from adsingestp.parsers.crossref import CrossrefParser
from adsingestp.parsers.base import BaseBeautifulSoupParser
from glob import glob
from adsputils import load_config, setup_logging

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
conf = load_config(proj_home=proj_home)
logger = setup_logging('completeness-statistics-pipeline', proj_home=proj_home,
                       level=conf.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=conf.get('LOG_STDOUT', False))

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


def process_one_meta_xml(filename):
    try:
        record = dict()
        with open(filename,'r') as fx:
            data = fx.read()
            try:
                parser = CrossrefParser()
                record = parser.parse(data)
            except Exception as err:
                raise CrossRefParseException(err)
            else:
                if record:
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

# loading bibcode-doi and bibstem-issn data into postgres

def load_classic_doi_bib_map(infile):
    # Classic: DOI-bibcode mapping
    records_bib_doi = list()
    ignorecount = 0
    found_doi = dict()
    try:
        with open(infile, 'r') as fa:
            for l in fa.readlines():
                try:
                    (bibcode, doi) = l.strip().split('\t')
                    if not found_doi.get(doi, None):
                        records_bib_doi.append({"doi": doi,
                                                "identifier": bibcode})
                        found_doi[doi] = 1
                    else:
                        logger.debug("Duplicate doi detected: (%s, %s)" %
                                          (bibcode, doi))
                except Exception as err:
                    logger.warning("bad line in %s: %s" % (infile, err))
        found_doi = None
    except Exception as err:
        raise LoadClassicDataException("Unable to load classic dois and bibcodes! %s" % err)
    return records_bib_doi

def load_journalsdb_issn_bibstem_list(infile):
    records_issn_bibstem = list()
    issn_dups = dict()
    try:
        with open(infile, 'r') as fi:
            for l in fi.readlines():
                (bibstem, issntype, issn) = l.strip().split('\t')
                if not issn_dups.get(issn, None):
                    issn_dups[issn] = 1
                    records_issn_bibstem.append({'issn': issn,
                                                 'bibstem': bibstem,                                                             'issn_type': issntype})
                else:
                    logger.debug("ISSN %s is a duplicate!" % issn)
    except Exception as err:
        raise LoadIssnDataException('Unable to load bibstem-issn map: %s' % err)
    return records_issn_bibstem


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
                    # pass
                    noncbib = l.strip()
                    canonical = 'none'
                if not noncbibcodes.get(noncbib, None):
                    noncbibcodes[noncbib] = canonical
    except Exception as err:
        raise LoadClassicDataException("Unable to load noncanonical bibcodes from %s: %s" % (bibfile, err))
    else:
        return noncbibcodes


def merge_bibcode_lists(canonicalfile, alternatefile, deletedfile, allfile):
    records_merged_bibcodes = list()
    try:
        canonical_bibs_list = load_classic_canonical_list(canonicalfile)
        alternate_bibs_dict = load_classic_noncanonical_bibs(alternatefile)
        deleted_bibs_dict = load_classic_noncanonical_bibs(deletedfile)
        all_bibs_dict = load_classic_noncanonical_bibs(allfile)
        merged = dict()
        for can in canonical_bibs_list:
            if not merged.get(can, None):
                records_merged_bibcodes.append({"identifier": can,
                                                "canonical_id": can,
                                                "idtype": "canonical"})
                merged[can] = 1
        for alt, can in alternate_bibs_dict.items():
            if not merged.get(alt, None):
                records_merged_bibcodes.append({"identifier": alt,
                                                "canonical_id": can,
                                                "idtype": "alternate"})
                merged[alt] = 1
        for dlt, can in deleted_bibs_dict.items():
            if not merged.get(dlt, None):
                records_merged_bibcodes.append({"identifier": dlt,
                                                "canonical_id": can,
                                                "idtype": "deleted"})
                merged[dlt] = 1
        for oth, can in all_bibs_dict.items():
            if not merged.get(oth, None):
                if can == "none":
                    records_merged_bibcodes.append({"identifier": oth,
                                                    "canonical_id": can,
                                                    "idtype": "noindex"})
                else:
                    records_merged_bibcodes.append({"identifier": oth,
                                                    "canonical_id": can,
                                                    "idtype": "other"})
                merged[oth] = 1
        merged = None
    except Exception as err:
        raise MergeClassicDataException("Unable to merge bibcodes lists: %s" % err)
    return records_merged_bibcodes


def get_completeness_fraction(byVolumeData):
    totals = dict()
    matches = ["canonical","partial","alternate","deleted"]
    unmatches = ["mismatch","unmatched"]
    totalMatch = 0
    totalUnmatch = 0
    totalNoIndex = 0
    try:
        for rec in byVolumeData:
            match = rec.get('matchtype')
            status = rec.get('status')
            count = rec.get('count')
            if match in matches:
                totalMatch += count
            elif match in unmatches:
                totalUnmatch += count
            elif status == 'NoIndex':
                totalNoIndex += count
        totalIndexable = totalMatch + totalUnmatch
        completenessFraction = totalMatch / totalIndexable
        return (totalIndexable, completenessFraction)
    except Exception as err:
        raise CompletenessFractionException("Unable to calculate completeness: %s" % err)


def export_completeness_data(allData, outfile):
    if not outfile:
        raise MissingFilenameException("Completeness JSON filename location not configured.")
    else:
        try:
            with open(outfile, 'w') as fj:
                fj.write(json.dumps(allData))
        except Exception as err:
            raise JsonExportException(err)
