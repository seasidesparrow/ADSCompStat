import json
import math
import os

from adsenrich.bibcodes import BibcodeGenerator
from kombu import Queue
from sqlalchemy import func

from adscompstat import app as app_module
from adscompstat import utils
from adscompstat.exceptions import BibstemLookupException, FetchClassicBibException
from adscompstat.match import CrossrefMatcher
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatIssnBibstem as issn_bibstem
from adscompstat.models import CompStatMaster as master
from adscompstat.models import CompStatSummary as summary

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
app = app_module.ADSCompStatCelery(
    "completeness-statistics-pipeline",
    proj_home=proj_home,
    config=globals().get("config", {}),
    local_config=globals().get("local_config", {}),
)
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue("write-db", app.exchange, routing_key="write-db"),
    Queue("get-logfiles", app.exchange, routing_key="get-logfiles"),
    Queue("process-meta", app.exchange, routing_key="process-meta"),
    Queue("compute-stats", app.exchange, routing_key="compute-stats"),
)


class DataBaseSession(object):

    def __init__(self):
        self.session = app.session_scope()

    def _query_master_by_doi(self, doi):
        return self.session.query(master.master_doi).filter_by(master_doi=doi).all()

    def _update_master_by_doi(self, row_modeldict):
        doi = row_modeldict.get("master_doi", None)
        try:
            self.session.query(master).filter_by(master_doi=doi).update(row_modeldict)
            self.session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            logger.warning("DB write error: %s; Record: %s" % (err, record))

    def _query_bibstem_by_issn(self, issn):
        try:
            results = self.session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn == issnString).first()
            return results
        except Exception as err:
            logger.warning("Unable to get bibstem from issn %s: %s" % (issn, err))
        
    def _query_completeness_per_bibstem(self, bibstem):
        try:
            result = (
                self.session.query(
                    func.substr(master.bibcode_meta, 10, 5),
                    master.status,
                    master.matchtype,
                    func.count(master.bibcode_meta),
                )
                .filter(func.substr(master.bibcode_meta, 5, 5) == bibstem)
                .group_by(func.substr(master.bibcode_meta, 10, 5), master.status, master.matchtype)
                .all()
            )
            return result
        except Exception as err:
            logger.warning("Error querying completeness for bibstem %s: %s" % (bibstem, err))


    def _query_classic_bibcodes(self, doi, bibcode):
        bibcodesFromDoi = []
        bibcodesFromBib = []
        try:
            bibcodesFromDoi = (
                self.session.query(
                    alt_identifiers.identifier,
                    alt_identifiers.canonical_id,
                    alt_identifiers.idtype,
                )
                .join(identifier_doi, alt_identifiers.canonical_id == identifier_doi.identifier)
                .filter(identifier_doi.doi == doi)
                .all()
            )
            if bibcode:
                bibcodesFromBib = (
                    self.session.query(
                        alt_identifiers.identifier,
                        alt_identifiers.canonical_id,
                        alt_identifiers.idtype,
                    )
                    .filter(alt_identifiers.identifier == bibcode)
                    .all()
                )
        except Exception as err:
            raise FetchClassicBibException(err)
        else:
            return bibcodesFromDoi, bibcodesFromBib
      
    def _write_completeness_summary(self, summary):
        try:
            self.session.add(summary)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            logger.warning(
                "Error writing summary data for %s, v %s: %s" % (bibstem, k, err)
            )

    def _query_retry_files(self, rec_type):
        try:
            result = self.session.query(master.harvest_filepath).filter(master.matchtype == rec_type).all()
            )
            return result
        except Exception as err:
            logger.error("Unable to retrieve retry files of type %s: %s" % (rec_type, err))

    def _query_summary_bibstems(self):
        try:
            bibstems = self.session.query(summary.bibstem).distinct().all()
            bibstems = [x[0] for x in bibstems]
            return bibstems
        except Exception as err:
            logger.error("Failed to get bibstems from summary: %s" % err)

    def _query_summary_single_bibstem(self, bibstem):
        try:
            result = self.session.query(
                    summary.bibstem,
                    summary.volume,
                    summary.complete_fraction,
                    summary.paper_count,
                )
                .filter(summary.bibstem == bibstem)
                .all()
            )
            return result
        except Exception as err:
            logger.error("Failed to get completeness for bibstem %s: %s" % (bibstem, err))



related_bibstems = []
related_bibs_file = app.conf.get("JOURNALSDB_RELATED_BIBSTEMS", None)
if related_bibs_file:
    try:
        with open(related_bibs_file, "r") as fj:
            data = json.load(fj)
            related_bibstems = data.get("related_bibstems", [])
    except Exception as err:
        logger.warning("Unable to load related bibstems list: %s" % err)
else:
    logger.warning("Related bibstems filename not set.")


# No delay/queue, synchronous only
def task_clear_classic_data():
    db = DataBaseSession()
    try:
        db.session.query(identifier_doi).delete()
        db.session.query(alt_identifiers).delete()
        db.session.query(issn_bibstem).delete()
        db.session.commit()
        logger.info("Existing classic data tables cleared.")
    except Exception as err:
        db.session.rollback()
        db.session.flush()
        logger.error("Failed to clear classic data tables: %s" % err)


# No delay/queue, synchronous only
def task_write_block_to_db(table, datablock):
    db = DataBaseSession()
    try:
        db.session.bulk_insert_mappings(table, datablock)
        db.session.commit()
    except Exception as err:
        db.session.rollback()
        db.session.flush()
        logger.warning("Failed to write data block: %s" % err)


@app.task(queue="write-db")
def task_write_matched_record_to_db(record):
    if record:
        doi = record[1]
        row = master(
            harvest_filepath=record[0],
            master_doi=record[1],
            issns=record[2],
            db_origin="Crossref",
            master_bibdata=record[3],
            classic_match=record[4],
            status=record[5],
            matchtype=record[6],
            bibcode_meta=record[7],
            bibcode_classic=record[8],
            notes=record[9],
        )
        db = DataBaseSession()
        try:
            result = db._query_master_by_doi(doi)
            if not result:
                db.session.add(row)
                db.session.commit()
            else:
                db._update_master_by_doi(row)
        except Exception as err:
            db.session.rollback()
            db.session.flush()
            logger.error("DB: failed to add/update row in master: %s" % err)
    else:
        logger.info("Null record passed to write-db")


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
                task_process_meta.delay(batch)
                batch = []
        if len(batch):
            logger.debug("Calling task_process_meta with batch '%s'" % batch)
            task_process_meta.delay(batch)
    except Exception as err:
        logger.warning("Error processing logfile %s: %s" % (infile, err))


def db_query_bibstem(record):
    try:
        issn_list = record.get("publication", {}).get("ISSN", [])
        bibstem = ""
        for issn in issn_list:
            if not bibstem:
                issnString = str(issn.get("issnString", ""))
                if issnString:
                    if len(issnString) == 8:
                        issnString = issnString[0:4] + "-" + issnString[4:]
                    try:
                        db = DataBaseSession()
                        bibstem_result = (
                            db._query_bibstem_by_issn(issnString)
                        )
                        if bibstem_result:
                            bibstem = bibstem_result[0]
                    except Exception as err:
                        logger.warning("Error from database call: %s" % err)
    except Exception as err:
        raise BibstemLookupException(err)
    else:
        return bibstem


@app.task(queue="process-meta")
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
                logger.warning("Parsing failed for %s: %s" % (infile, err))
                doi = ""
                issns = json.dumps({})
                bibdata = json.dumps({})
                match = json.dumps({})
                status = "Failed"
                matchtype = "failed"
                bibcode = ""
                classic_bibcode = ""
                matchedRecord = (
                    infile,
                    doi,
                    issns,
                    bibdata,
                    match,
                    status,
                    matchtype,
                    bibcode,
                    classic_bibcode,
                    str(err),
                )
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
                    matchedRecord = (
                        infile,
                        doi,
                        issns,
                        bibdata,
                        match,
                        status,
                        matchtype,
                        bibcode,
                        classic_bibcode,
                        parsestatus,
                    )
                else:
                    try:
                        ingestRecord = processedRecord.get("record", "")
                        bibstem = db_query_bibstem(ingestRecord)
                        bibcode = bibgen.make_bibcode(ingestRecord, bibstem=bibstem)
                        doi = processedRecord.get("master_doi", "")
                        db = DataBaseSession()
                        (bibcodesFromDoi, bibcodesFromBib) = db._query_classic_bibcodes(
                            doi, bibcode
                        )
                        xmatch = CrossrefMatcher(related_bibstems=related_bibstems)
                        xmatchResult = xmatch.match(bibcode, bibcodesFromDoi, bibcodesFromBib)
                        if xmatchResult:
                            matchtype = xmatchResult.get("match", "")
                            if matchtype in [
                                "canonical",
                                "deleted",
                                "alternate",
                                "partial",
                                "other",
                                "mismatch",
                            ]:
                                status = "Matched"
                            else:
                                status = "Unmatched"
                            if matchtype == "Classic Canonical Bibcode":
                                matchtype = "other"
                            classic_match = xmatchResult.get("errs", {})
                            classic_bibcode = xmatchResult.get("bibcode", "")
                        else:
                            status = "NoIndex"
                            matchtype = "other"
                            classic_match = {}
                            classic_bibcode = ""

                        # create a postgres-ready record with matching result
                        # for the record in infile
                        issns = json.dumps(processedRecord.get("issns", {}))
                        bibdata = json.dumps(processedRecord.get("master_bibdata", {}))
                        match = json.dumps(classic_match)

                        matchedRecord = (
                            infile,
                            doi,
                            issns,
                            bibdata,
                            match,
                            status,
                            matchtype,
                            bibcode,
                            classic_bibcode,
                            "",
                        )
                    except Exception as err:
                        logger.warning("Crossref matching failed for %s: %s" % (infile, err))
                        doi = processedRecord.get("master_doi", "")
                        issns = json.dumps(processedRecord.get("issns", {}))
                        bibdata = json.dumps(processedRecord.get("master_bibdata", {}))
                        match = json.dumps({})
                        status = "Failed"
                        matchtype = "failed"
                        bibcode = ""
                        classic_bibcode = ""
                        matchedRecord = (
                            infile,
                            doi,
                            issns,
                            bibdata,
                            match,
                            status,
                            matchtype,
                            bibcode,
                            classic_bibcode,
                            str(err),
                        )
            if matchedRecord:
                task_write_matched_record_to_db.delay(matchedRecord)
            else:
                logger.warning("No matchedRecord generated for %s!" % infile)
    except Exception as err:
        logger.error("Record batch failed for %s: %s" % (infile_batch, err))


@app.task(queue="compute-stats")
def task_completeness_per_bibstem(bibstem):
    try:
        bibstem = bibstem.ljust(5, ".")
        db = DataBaseSession()
        result = (
            db._query_completeness_per_bibstem(bibstem)
        )
    except Exception as err:
        logger.warning("Failed to get completeness summary for bibstem %s: %s" % (bibstem, err))
    else:
        # result is an array of tuples with (bibstem+vol,status,matchtype,count)
        volumeSummary = dict()
        for r in result:
            vol = r[0]
            if vol[-1] not in ["L", "P", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
                vol = vol[0:-1]
            vol = vol.lstrip(".").rstrip(".")
            stat = r[1]
            mtype = r[2]
            count = r[3]
            if volumeSummary.get(vol, None):
                volumeSummary[vol].append({"status": stat, "matchtype": mtype, "count": count})
            else:
                volumeSummary[vol] = [{"status": stat, "matchtype": mtype, "count": count}]
        for k, v in volumeSummary.items():
            try:
                (countrecs, compfrac) = utils.get_completeness_fraction(v)
                outrec = summary(
                    bibstem=bibstem.rstrip("."),
                    volume=k,
                    paper_count=countrecs,
                    complete_fraction=compfrac,
                    complete_details=json.dumps(v),
                )
            except Exception as err:
                logger.warning(
                    "Error calculating summary completeness data for %s, v %s: %s"
                    % (bibstem, k, err)
                )
            else:
                db = DataBaseSession()
                db._write_completeness_summary(outrec)

def task_do_all_completeness():
    try:
        with app.session_scope() as session:
            bibstems = session.query(func.substr(master.bibcode_meta, 5, 5)).distinct().all()
            if bibstems:
                session.query(summary).delete()
                session.commit()
            else:
                logger.warning("Completeness summary table wasn't deleted")
        bibstems = [x[0] for x in bibstems]
        for bibstem in bibstems:
            task_completeness_per_bibstem.delay(bibstem)
    except Exception as err:
        logger.error("Failed to clear classic data tables: %s" % err)


def task_export_completeness_to_json():
    db = DataBaseSession()
    try:
        allData = []
        bibstems = db._query_summary_bibstems()
        for bib in bibstems:
            completeness = []
            result = db._query_summary_single_bibstem(bib)
            paperCount = 0
            averageCompleteness = 0.0
            for r in result:
                if type(r[2]) == float:
                    r2_export = math.floor(10000 * r[2] + 0.5) / 10000.0
                else:
                    r2_export = r[2]
                completeness.append({"volume": r[1], "completeness_fraction": r2_export})
                paperCount += r[3]
                averageCompleteness += r[3] * r[2]
            averageCompleteness = averageCompleteness / paperCount
            avg_export = math.floor(10000 * averageCompleteness + 0.5) / 10000.0
            allData.append(
                {
                    "bibstem": bib,
                    "completeness_fraction": avg_export,
                    "completeness_details": completeness,
                }
            )
        if allData:
            utils.export_completeness_data(
                allData, app.conf.get("COMPLETENESS_EXPORT_FILE", None)
            )
    except Exception as err:
        logger.error("Unable to export completeness data to disk: %s" % err)


@app.task(queue="get-logfiles")
def task_retry_records(rec_type):
    batch_count = app.conf.get("RECORDS_PER_BATCH", 100)
    try:
        db = DataBaseSession()
        result = db._query_retry_files(rec_type)
        batch = []
        for r in result:
            batch.append(r[0])
            if len(batch) == batch_count:
                logger.debug("Calling task_process_meta with batch '%s'" % batch)
                task_process_meta.delay(batch)
                batch = []
        if len(batch):
            logger.debug("Calling task_process_meta with batch '%s'" % batch)
            task_process_meta.delay(batch)
    except Exception as err:
        logger.warning('Error reprocessing records of matchtype "%s": %s' % (rec_type, err))
