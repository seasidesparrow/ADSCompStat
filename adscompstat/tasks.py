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
    batch_count = conf.get("RECORDS_PER_BATCH", 100)
    try: 
        files_to_process = utils.read_updateagent_log(infile)
        batch = []
        for xmlFile in files_to_process:
            xmlFilePath = app.conf.get("HARVEST_BASE_DIR", "/") + xmlFile
            batch.append(xmlFilePath)
            if len(batch) > batch_count:
                logger.debug("Calling task_parse_meta with batch '%s'" % batch)
                task_parse_meta.delay(batch)
                batch = []
        if len(batch):
            logger.debug("Calling task_parse_meta with batch '%s'" % batch)
            task_parse_meta.delay(batch)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))


@app.task(queue="parse-meta")
def task_parse_meta(infile_batch):
    try:
        failures = []
        batch_out = []
        for infile in infile_batch:
            try:
                record = utils.parse_one_meta_xml(infile)
                if record:
                    with app.session_scope() as session:
                        issn_list = record.get("publication", {}).get("ISSN", [])
                        bibstem = None
                        for issn in issn_list:
                            if not bibstem:
                                issnString = issn.get("issnString", None)
                                if issnString:
                                    bibstem = session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn==issn).first()
                            
                      
                else:
                    failures.append({"file": infile, "status": "parser failed"})
            except Exception as err:
                failures.append({"file": infile, "status": "error: %s" % err}
        
    except Exception as err:
        logger.error("Error processing record bundle: %s" % err)
