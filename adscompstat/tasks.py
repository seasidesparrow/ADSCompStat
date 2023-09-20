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
from sqlalchemy import insert, func

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
app = app_module.ADSCompStatCelery("completeness-statistics-pipeline", proj_home=proj_home, config=globals().get("config", {}), local_config=globals().get("local_config", {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue("get-logfiles", app.exchange, routing_key="get-logfiles"),
    Queue("parse-meta", app.exchange, routing_key="parse-meta"),
    Queue("match-classic", app.exchange, routing_key="match-classic"),
    Queue("write-db", app.exchange, routing_key="write-db"),
    # Queue("compute-stats", app.exchange, routing_key="compute-stats"),
)


def task_clear_classic_data():
    with app.session_scope() as session:
        try:
            session.query(identifier_doi).delete()
            session.query(alt_identifiers).delete()
            session.query(issn_bibstem).delete()
            session.commit()
            logger.info("Existing classic data tables cleared.")
        except Exception as err:
            session.rollback()
            session.commit()
            logger.error("Failed to clear classic data tables: %s" % err)


def task_write_block_to_db(table, datablock):
    with app.session_scope() as session:
        try:
            session.bulk_insert_mappings(table, datablock)
            session.commit()
        except Exception as err:
            session.rollback()
            session.commit()
            logger.error("Failed to write data block: %s" % err)

@app.task(queue="write-db")
def task_write_matched_record_to_db(record):
    with app.session_scope() as session:
        try:
            doi = record[1]
            result = session.query(master.master_doi).filter_by(master_doi=doi)
            if not result:
                row = master(harvest_filepath=record[0],
                             master_doi=record[1],
                             issns=record[2],
                             db_origin='Crossref',
                             master_bibdata=record[3],
                             classic_match=record[4],
                             status=record[5],
                             matchtype=record[6],
                             bibcode_meta=record[7],
                             bibcode_classic=record[8])
                session.add(row)
                session.commit()
            else:
                update = {"harvest_filepath":record[0],
                          "master_doi":record[1],
                          "issns": record[2],
                          "db_origin": "Crossref",
                          "master_bibdata": record[3],
                          "classic_match": record[4],
                          "status": record[5],
                          "matchtype": record[6],
                          "bibcode_meta": record[7],
                          "bibcode_classic": record[8]}
                session.query(master).filter_by(master_doi=doi).update(update)
                session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            logger.error("Error: %s; Record: %s" % (err, record))


@app.task(queue="get-logfiles")
def task_process_logfile(infile):
    """
    Parse one oaipmh harvesting logfile to retrieve newly downloaded records,
    and forward batches of those records to task_process_meta().  The filename
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
                logger.debug("Calling task_process_meta with batch '%s'" % batch)
                task_process_meta(batch)
                batch = []
        if len(batch):
            logger.debug("Calling task_process_meta with batch '%s'" % batch)
            task_process_meta(batch)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))


def db_query_bibstem(record):
    try:
        with app.session_scope() as session:
            issn_list = record.get("publication", {}).get("ISSN", [])
            bibstem = ""
            for issn in issn_list:
                if not bibstem:
                    issnString = issn.get("issnString", "")
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


def db_query_classic_bibcodes(doi, bibcode):
    try:
        bibcodesFromDoi = []
        bibcodesFromBib = []
        with app.session_scope() as session:
            bibcodesFromDoi = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).join(identifier_doi, alt_identifiers.canonical_id == identifier_doi.identifier).filter(identifier_doi.doi == doi).all()
        if bibcode:
            bibcodesFromBib = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).filter(alt_identifiers.identifier == bibcode).all()
    except Exception as err:
        raise FetchClassicBibException(err)
    else:
        return bibcodesFromDoi, bibcodesFromBib


@app.task(queue="parse-meta")
def task_process_meta(infile_batch):
    """
    Parses a batch of crossref xml files from the OAIPMH harvester into an
    ingestDataModel object, and then extracts and reformats the records'
    metadata into a format the classic matcher can interpret and store.
    Batched output and failures are sent for matching or special handling.
    """

    try:
        bibgen = BibcodeGenerator()
        for infile in infile_batch:
            matchedRecord = ""
            # For each metadata.xml file: parse it, try to make a bibcode,
            # and prep the result for task_classic_match
            try:
                processedRecord = utils.process_one_meta_xml(infile)
            except Exception as err:
                logger.error("Parsing failed for %s: %s" % (infile, err))
                doi = ""
                issns = json.dumps({})
                bibdata = json.dumps({})
                match = json.dumps({})
                status = "Failed"
                matchtype = "failed"
                bibcode = ""
                classic_bibcode = ""
                matchedRecord = (infile,
                                 doi,
                                 issns,
                                 bibdata,
                                 match,
                                 status,
                                 matchtype,
                                 bibcode,
                                 classic_bibcode,
                                 str(err))
            else:
                parsestatus = processedRecord.get("status", "")
                # If there's a status field, it means processing failed and
                # you need to write a placeholder record for the file.
                if parsestatus:
                    doi = processedRecord.get("master_doi", "")
                    issns = json.dumps(processedRecord.get("issns", {}))
                    bibdata = json.dumps(processedRecord.get("master_bibdata", {}))
                    match = json.dumps({})
                    status = "Failed"
                    matchtype = "failed"
                    bibcode = ""
                    classic_bibcode = ""
                    matchedRecord = (infile,
                                     doi,
                                     issns,
                                     bibdata,
                                     match,
                                     status,
                                     matchtype,
                                     bibcode,
                                     classic_bibcode,
                                     parsestatus)
                else:
                    try:
                        ingestRecord = processedRecord.get("record", "")
                        bibstem = db_query_bibstem(ingestRecord)
                        bibcode = bibgen.make_bibcode(ingestRecord,
                                                      bibstem=bibstem)
                        doi = processedRecord.get("master_doi", "")
                        (bibcodesFromDoi, bibcodesFromBib) = db_query_classic_bibcodes(doi, bibcode)
                        xmatch = CrossrefMatcher()
                        xmatchResult = xmatch.match(bibcode,
                                                    bibcodesFromDoi,
                                                    bibcodesFromBib)
                        if xmatchResult:
                            matchtype = xmatchResult.get("match", "")
                            if matchtype in ["canonical", "deleted", "alternate", "partial", "other", "mismatch"]:
                                status = "Matched"
                            else:
                                status = "Unmatched"
                            if matchtype == "Classic Canonical Bibcode":
                                matchtype = "other"
                            classic_match = xmatchResult.get("errs", {})
                            classic_bibcode = xmatchResult.get("bibcode", "")
                        else:
                            status="NoIndex"
                            matchtype = "other"
                            classic_match = {}
                            classic_bibcode = ""

                        # create a postgres-ready record with matching result
                        # for the record in infile
                        issns = json.dumps(processedRecord.get("issns", {}))
                        bibdata = json.dumps(processedRecord.get("master_bibdata", {}))
                        match = json.dumps(classic_match)

                        matchedRecord = (infile,
                                         doi,
                                         issns,
                                         bibdata,
                                         match,
                                         status,
                                         matchtype,
                                         bibcode,
                                         classic_bibcode,
                                         "")
                    except Exception as err:
                        logger.error("Crossref matching failed for %s: %s" % (infile, err))
                        doi = processedRecord.get("master_doi", "")
                        issns = json.dumps(processedRecord.get("issns", {}))
                        bibdata = json.dumps(processedRecord.get("master_bibdata", {}))
                        match = json.dumps({})
                        status = "Failed"
                        matchtype = "failed"
                        bibcode = ""
                        classic_bibcode = ""
                        matchedRecord = (infile,
                                         doi,
                                         issns,
                                         bibdata,
                                         match,
                                         status,
                                         matchtype,
                                         bibcode,
                                         classic_bibcode,
                                         str(err))
            if matchedRecord:
                task_write_matched_record_to_db(matchedRecord)
            else:
                logger.error("No matchedRecord generated for %s!" % infile)
    except Exception as err:
        logger.error("Record batch failed for %s: %s" % (infile_batch, err))
