import json
import os
from kombu import Queue
from adscompstat.models import CompStatMaster as master
from adscompstat.models import CompStatSummary as summary
from adscompstat import app as app_module
from adscompstat import utils
from adscompstat.bibcodes import BibcodeGenerator
from adscompstat.match import CrossrefMatcher
from adscompstat.exceptions import *

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCompStatCelery('compstat-pipeline', proj_home=proj_home, config=globals().get('config', {}), local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('parse-meta', app.exchange, routing_key='parse-meta'),
    Queue('match-classic', app.exchange, routing_key='match-classic'),
    Queue('compute-stats', app.exchange, routing_key='calc-completeness')
)

try:
    xmatch = CrossrefMatcher()
    bibgen = BibcodeGenerator()
except Exception as err:
    raise NoDataHandlerException(err)

@app.task(queue='compute-stats')
def task_write_result_to_db(inrec):
    with app.session_scope() as session:
        try:
            outrec = master(harvest_filepath=inrec[0],
                            master_doi=inrec[1],
                            issns=inrec[2],
                            db_origin='Crossref',
                            master_bibdata=inrec[3],
                            classic_match=inrec[4],
                            status=inrec[5],
                            matchtype=inrec[6])
            session.add(outrec)
            session.commit()
        except Exception as err:
            logger.error("Problem with database commit: %s" % err)

@app.task(queue='match-classic')
def task_match_record_to_classic(processingRecord):
    allowedMatchType = ['Exact', 'Deleted', 'Alternate', 'Partial', 'Other']
    try:
        record = processingRecord.get('record', None)
        recBibcode = bibgen.make_bibcode(record)
        recDOI = processingRecord.get('doi', None)
        xmatchResult = xmatch.match(recDOI, recBibcode)
        status = 'Unmatched'
        matchtype = xmatchResult.get('match', None)
        harvest_filepath = processingRecord.get('harvest_filepath', None)
        logger.info("harvest_filepath type is %s" % type(harvest_filepath))
        master_doi = processingRecord.get('master_doi', None)
        if matchtype in allowedMatchType:
            status = 'Matched'
        classic_match = xmatchResult.get('errs', None)
        if type(classic_match) == dict:
            classic_match = json.dumps(classic_match)
        issns = processingRecord.get('issns', None)
        if type(issns) == dict:
            issns = json.dumps(issns)
        master_bibdata = processingRecord.get('master_bibdata', None)
        logger.warn("master_bibdata type is %s" % type(master_bibdata))
        if type(master_bibdata) == dict:
            master_bibdata = json.dumps(master_bibdata)
    except Exception as err:
        logger.warn("Error matching record: %s" % err)
    else:
        try:
            outputRecord = (harvest_filepath,
                            master_doi,
                            issns,
                            master_bibdata,
                            classic_match,
                            status,
                            matchtype)
            logger.info("outputRecord: %s" % str(outputRecord))
            task_write_result_to_db.delay(outputRecord)
        except Exception as err:
            logger.warn("Error creating a models record: %s" % err)

@app.task(queue='parse-meta')
def task_add_bibcode(outputRecord):
    publisherRecord = outputRecord.get('pubRec', None)
    if publisherRecord:
        try:
            sourceFile = outputRecord.get('sourceFile', None)
            bibcode = bibgen.make_bibcode(record)
        except Exception as err:
            logger.warn("Failed to create bibcode for file %s: %s" % (sourceFile, err))
            bibcode = None
        outputRecord['bibcode'] = bibcode
        task_match_record_to_classic.delay(outputRecord)

@app.task(queue='parse-meta')
def task_process_metafile(infile):
    record = utils.parse_one_meta_xml(infile)
    try:
        publication = record.get('publication', None)
        pagination = record.get('pagination', None)
        pids = record.get('persistentIDs', None)
        first_author = record.get('authors', None)
        title = record.get('title', None)
        if publication:
            pub_year = publication.get('pubYear', None)
            issns = publication.get('ISSN', None)
        if pids:
            for pid in pids:
                if pid.get('DOI', None):
                    doi = pid.get('DOI', None)
        if first_author:
            first_author = first_author[0]
    except Exception as err:
        logger.warning("Unable to process metafile %s: %s" % (infile, err))
    else:
        bib_data = {'publication': publication,
                   'pagination': pagination,
                   'persistentIDs': pids,
                   'first_author': first_author,
                   'title': title}
        processingRecord = {'record': record,
                            'harvest_filepath': infile,
                            'master_doi': doi,
                            'issns': json.dumps(issns),
                            'master_bibdata': json.dumps(bib_data)}
        task_match_record_to_classic.delay(processingRecord)

@app.task(queue='parse-meta')
def task_process_logfile(infile):
    try:
        files_to_process = utils.read_updateagent_log(infile)
        for xmlFile in files_to_process:
            xmlFile = app.conf.get('HARVEST_BASE_DIR', '/') + xmlFile
            try:
                task_process_metafile.delay(xmlFile)
            except Exception as err:
                logger.warn("error processing xmlFile %s: %s" % (xmlFile, err))
    except Exception as err:
        logger.warn("error processing logfile %s: %s" % (infile, err))
