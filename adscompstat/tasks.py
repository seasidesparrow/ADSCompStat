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
app = app_module.ADSCompStatCelery("compstat-pipeline", proj_home=proj_home, config=globals().get("config", {}), local_config=globals().get("local_config", {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue("parse-metafile", app.exchange, routing_key="parse-metafile"),
    Queue("add-emptyrecord", app.exchange, routing_key="add-emptyrecord"),
    Queue("compute-stats", app.exchange, routing_key="compute-stats"),
    Queue("output-metadata", app.exchange, routing_key="output-metadata"),
    Queue("get-logfiles", app.exchange, routing_key="get-logfiles")
)

@app.task(queue="get-logfiles")
def task_process_logfile(infile):
    try: 
        files_to_process = utils.read_updateagent_log(infile)
        for xmlFile in files_to_process:
            xmlFilePath = app.conf.get("HARVEST_BASE_DIR", "/") + xmlFile
            logger.debug("I have a file name: %s" % xmlFilePath)
            # task_process_metafile.delay(xmlFile)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))
