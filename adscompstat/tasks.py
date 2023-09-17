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
    # Queue("match-classic", app.exchange, routing_key="match-classic"),
    # Queue("compute-stats", app.exchange, routing_key="compute-stats"),
    # Queue("output-metadata", app.exchange, routing_key="output-metadata"),
)


@app.task(queue="get-logfiles")
def task_process_logfile(infile):
    """
    Parse one oaipmh harvesting logfile to retrieve newly downloaded records,
    and forward batches of those records to task_parse_meta().  The filename
    in the logfile assumes the same HARVEST_BASE_DIR as the logfiles
    themselves, and prepends the full path to the relative path in the file.

    Parameters:
    infile (string): path to one logfile

    Returns:
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
            try:
                record = utils.parse_one_meta_xml(infile)
                if record:
                    bibstem = _fetch_bibstem(record)
                    if bibstem:
                        bibcode = bibgen.make_bibcode(record, bibstem=bibstem)
                        logger.debug("Got bibcode from %s: %s" % 
                                         (infile, bibcode) )
                    else:
                        logger.debug("No bibcode from record %s" % infile)
                else:
                    failures.append({"file": infile, "status": "parser failed"})
            except Exception as err:
                failures.append({"file": infile, "status": "error: %s" % err})
        batch_size = len(infile_batch)
        if failures:
           fail_size = len(failures)
           logger.warning("Failed records: %s of %s records failed in this batch." % (fail_size, batch_size))
           logger.debug("Failures: %s" % str(failures))
        else:
           logger.info("No (0) failed records in batch (%s)." % batch_size)
    except Exception as err:
        logger.error("Error processing record batch %s: %s" % (infile_batch, err) )
