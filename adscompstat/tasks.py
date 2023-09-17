import json
import os
from kombu import Queue
from adscompstat.models import CompStatMaster as master
from adscompstat.models import CompStatSummary as summary
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIssnBibstem as issn_bibstem
from adscompstat import app as app_module
from adscompstat import utils
from adsenrich.bibcodes import BibcodeGenerator
from adscompstat.match import CrossrefMatcher
from adscompstat.exceptions import *
from sqlalchemy import func

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
app = app_module.ADSCompStatCelery("completeness-statistics-pipeline", proj_home=proj_home, config=globals().get("config", {}), local_config=globals().get("local_config", {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue("get-logfiles", app.exchange, routing_key="get-logfiles"),
    Queue("parse-meta", app.exchange, routing_key="parse-meta"),
    Queue("match-classic", app.exchange, routing_key="match-classic"),
    # Queue("compute-stats", app.exchange, routing_key="compute-stats"),
    # Queue("write-db", app.exchange, routing_key="write-db"),
)


def task_clear_classic_data():
    with app.session_scope() as session:
        try:            
            session.query(identifier_doi).delete()
            session.query(alt_identifiers).delete()
            session.query(issn_bibstem).delete()
            session.commit()
        except Exception as err:
            session.rollback()
            session.commit()
            logger.error("Failed to clear classic data tables: %s" % err)


@app.task(queue="write-db")
def task_write_block_to_db(table, datablock):
    try:
        with app.session_scope() as session:
            session.bulk_insert_mappings(table, datablock)
            session.commit()
    except Exception as err:
        logger.error("Failed to write data block: %s" % err)


def task_load_classic_data():
    blocksize = app.conf.get("CLASSIC_DATA_BLOCKSIZE", 10000)
    try:
        task_clear_classic_data()
    except Exception as err:
        logger.error("Failed to clear classic data tables: %s" % err)
    else:

        # Bibstem to ISSN mapping
        try:
            table = issn_bibstem
            infile = app.conf.get("JOURNALSDB_ISSN_BIBSTEM", None)
            records = utils.load_journalsdb_issn_bibstem_list(infile)
            if records:
                task_write_block_to_db.delay(table, records)
        except Exception as err:
            logger.error("Failed to load ISSN-to-bibstem mapping: %s" % err)

        # DOI-Bibcode mapping from classic
        try:
            table = identifier_doi
            infile = app.conf.get("CLASSIC_DOI_FILE", None)
            records = utils.load_classic_doi_bib_dict(infile)
            if records:
                insertlist = []
                for doi, bibc in records.items():
                    rec = {"doi": doi, "identifier": bibc}
                    insertlist.append(rec)
                while insertlist:
                    insertblock = insertlist[0:blocksize]
                    insertlist = insertlist[blocksize:]
                    task_write_block_to_db.delay(table, insertblock)
            else:
                logger.error("No data from DOI-Bibcode file %s" % infile)
        except Exception as err:
            logger.error("Failed to load DOI-Bibcode mapping: %s" % err)

        # Alternate & deleted bibcode mappings
        try:
            table = alt_identifiers
            infile_can = app.conf.get("CLASSIC_CANONICAL", None)
            infile_alt = app.conf.get("CLASSIC_ALTBIBS", None)
            infile_del = app.conf.get("CLASSIC_DELBIBS", None)
            infile_all = app.conf.get("CLASSIC_ALLBIBS", None)
            records = utils.merge_bibcode_lists(infile_can, infile_alt,
                                                infile_del, infile_all)
            if records:
                insertlist = []
                for k, v in records.items():
                    ident = k
                    canid = v.get('canonical_id', None)
                    idtype = v.get('idtype', None)
                    if canid and idtype:
                        rec = {'identifier': ident,
                               'canonical_id': canid,
                               'idtype': idtype}
                        insertlist.append(rec)
                while insertlist:
                    insertblock = insertlist[0:blocksize]
                    insertlist = insertlist[blocksize:]
                    task_write_block_to_db.delay(table, insertblock)
            else:
                logger.error("No data from canonical/alt/deleted bibcode maps")
        except Exception as err:
            logger.error("Failed to load Canonical to Alt/Del bibcode maps: %s" % err)


@app.task(queue="get-logfiles")
def task_process_logfile(infile):
    """
    Parse one oaipmh harvesting logfile to retrieve newly downloaded records,
    and forward batches of those records to task_parse_meta().  The filename
    in the logfile assumes the same HARVEST_BASE_DIR as the logfiles
    themselves, and prepends the full path to the relative path in the file.

    Parameters:
    infile (string): path to one logfile
    """

    batch_count = app.conf.get("RECORDS_PER_BATCH", 100)
    try: 
        files_to_process = utils.read_updateagent_log(infile)
        batch = []
        for xmlFile in files_to_process:
            xmlFilePath = app.conf.get("HARVEST_BASE_DIR", "/") + xmlFile
            batch.append(xmlFilePath)
            if len(batch) == batch_count:
                logger.debug("Calling task_parse_meta with batch '%s'" % batch)
                task_parse_meta(batch)
                batch = []
        if len(batch):
            logger.debug("Calling task_parse_meta with batch '%s'" % batch)
            task_parse_meta(batch)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))


def _fetch_bibstem(record):
    try:
        with app.session_scope() as session:
            issn_list = record.get("publication", {}).get("ISSN", [])
            bibstem = None
            for issn in issn_list:
                if not bibstem:
                    issnString = issn.get("issnString", None)
                    if issnString:
                        try:
                            bibstem_result = session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn==issnString).first()
                            if bibstem_result:
                                bibstem = bibstem_result[0]
                        except Exception as err:
                            logger.warning("Error from database call: %s" % err)
    except Exception as err:
        raise BibstemLookupException(err)
    else:
        return bibstem

@app.task(queue="parse-meta")
def task_parse_meta(infile_batch):
    try:
        failures = []
        batch_out = []
        bibgen = BibcodeGenerator()
        for infile in infile_batch:
            # For each metadata.xml file: parse it, try to make a bibcode,
            # and prep the result for task_classic_match
            try:
                record = utils.parse_one_meta_xml(infile)
                if record:
                    # try making a bibcode
                    bibstem = _fetch_bibstem(record)
                    if bibstem:
                        bibcode = bibgen.make_bibcode(record, bibstem=bibstem)
                        logger.debug("Got bibcode from %s: %s" % 
                                         (infile, bibcode) )
                    else:
                        logger.debug("No bibcode from record %s" % infile)

                    # field the bib data parsed from the record into an
                    # processedRecord to be sent to task_match_with_classic
                    publication = record.get("publication", None)
                    first_author = record.get("authors", [])[0]
                    title = record.get("title", None)
                    pagination = record.get("pagination", None)
                    pids = record.get("persistentIDs", None)
                    if pids:
                        doi = None
                        for pid in pids:
                            if pid.get("DOI", None):
                                doi = pid.get("DOI", None)
                    if not doi:
                        failures.append({"file": infile,
                                         "status": "No DOI found"})
                    else:
                        if publication:
                            pub_year = publication.get("pubYear", None)
                        else:
                            pub_year = None
                        bib_data = {"publication": publication,
                                    "pagination": pagination,
                                    "persistentIDs": pids,
                                    "first_author": first_author,
                                    "title": title}
                        processedRecord = {"harvest_filepath": infile,
                                           "master_doi": doi,
                                           "master_bibcode": bibcode,
                                           "master_bibdata": bib_data}
                        batch_out.append(processedRecord)
                else:
                    failures.append({"file": infile, 
                                     "status": "parser failed"})
            except Exception as err:
                failures.append({"file": infile, 
                                 "status": "error: %s" % err})

        # finish logging for the incoming batch
        batch_size = len(infile_batch)
        if failures:
           fail_size = len(failures)
           logger.error("Failed records: %s of %s records failed in this batch." % (fail_size, batch_size))
           logger.error("Failures: %s" % str(failures))
        else:
           logger.info("No (0) failed records in batch (%s)." % batch_size)

        # forward the successfully parsed records
        if batch_out:
            logger.info("Forwarding %s records to match_with_classic" %
                            len(batch_out))
            task_match_with_classic.delay(batch_out)
        else:
            logger.error("No records to match from this batch: %s" %
                               infile_batch)
    except Exception as err:
        logger.error("Error processing record batch %s: %s" %
                         (infile_batch, err))


def _fetch_classic_bibcodes(doi, bibcode):
    try:
        bibcodesFromXDoi = []
        bibcodesFromXBib = []
        with app.session_scope() as session:
            bibcodesFromXDoi = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).join(identifier_doi, alt_identifiers.canonical_id == identifier_doi.identifier).filter(identifier_doi.doi == doi).all()
        if bibcode:
            bibcodesFromXBib = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).filter(alt_identifiers.identifier == bibcode).all()
    except Exception as err:
        raise FetchClassicBibException(err)
    else:
        return bibcodesFromXDoi, bibcodesFromXBib


@app.task(queue="match-classic")
def task_match_with_classic(record_batch):
    try:
        failures = []
        matches = []
        for processedRecord in record_batch:
            try:
                doi = processedRecord.get("master_doi", None)
                bibcode = processedRecord.get("master_bibcode", None)
                (BibcodesDoi, BibcodesBib) = _fetch_classic_bibcodes(doi, bibcode)
            except Exception as err:
                logger.error("Failed to get classic data for %s: %s" %
                                 (processedRecord, err))
            else:
                xmatch = CrossrefMatcher()
                xmatchResult = xmatch.match(bibcode,
                                            BibcodesDoi,
                                            BibcodesBib)
                if xmatchResult:
                    matchtype = xmatchResult.get("match", None)
                    if matchtype in ["canonical", "deleted", "alternate", "partial", "other", "mismatch"]:
                        status = "Matched"
                    else:
                        status = "Unmatched"
                    if matchtype == "Classic Canonical Bibcode":
                        matchtype = "other"
                    classic_match = xmatchResult.get("errs", {})
                    classic_bibcode = xmatchResult.get("bibcode", None)
                else:
                    status="NoIndex"
                    matchtype = "other"
                    classic_match = {}
                    classic_bibcode = None

                harvest_filepath = processedRecord.get("harvest_filepath", None)
                recBibcode = processedRecord.get("bibcode", None)
                master_doi = processedRecord.get("master_doi", None)
                if type(classic_match) == dict:
                    classic_match = json.dumps(classic_match)
                issns = processedRecord.get("issns", {})
                if type(issns) == dict:
                    issns = json.dumps(issns)
                master_bibdata = processedRecord.get("master_bibdata", {})
                if type(master_bibdata) == dict:
                    master_bibdata = json.dumps(master_bibdata)
                matchedRecord = (harvest_filepath,
                                 master_doi,
                                 issns,
                                 master_bibdata,
                                 classic_match,
                                 status,
                                 matchtype,
                                 recBibcode,
                                 classic_bibcode)
                matches.append(matchedRecord)

            if matches:
                task_write_results_to_master.delay(matches)
        except Exception as err:
            logger.warning("Error creating a master record, record not added to database: %s" % err)



    except Exception as err:
        logger.error("Error matching record batch %s: %s" %
                         (record_batch, err))





