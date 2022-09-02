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
    Queue('compute-stats', app.exchange, routing_key='compute-stats'),
    Queue('get-logfiles', app.exchange, routing_key='get-logfiles'),
    Queue('add-bibcode', app.exchange, routing_key='add-bibcode'),
    Queue('output-metadata', app.exchange, routing_key='output-metadata')
)

try:
    i2b = app.conf.get('ISSN2BIBSTEM', None)
    n2b = app.conf.get('NAME2BIBSTEM', None)
    xmatch = CrossrefMatcher()
    bibgen = BibcodeGenerator(issn2bibstem=i2b, name2bibstem=n2b)
except Exception as err:
    raise NoDataHandlerException(err)

@app.task(queue='output-metadata')
def task_write_result_to_db(inrec):
    with app.session_scope() as session:
        try:
            checkdoi = inrec[1]
            result = session.query(master.master_doi).filter_by(master_doi=checkdoi).all()
            if not result:
                outrec = master(harvest_filepath=inrec[0],
                                master_doi=inrec[1],
                                issns=inrec[2],
                                db_origin='Crossref',
                                master_bibdata=inrec[3],
                                classic_match=inrec[4],
                                status=inrec[5],
                                matchtype=inrec[6],
                                bibcode_meta=inrec[7],
                                bibcode_classic=inrec[8])
                session.add(outrec)
                session.commit()
            else:
                logger.info("Record for DOI %s exists already, ignoring for now." % checkdoi)
        except Exception as err:
            session.rollback()
            session.flush()
            logger.warning("Problem with database commit: %s" % err)

@app.task(queue='match-classic')
def task_match_record_to_classic(processingRecord):
    allowedMatchType = ['Exact', 'Deleted', 'Alternate', 'Partial', 'Other', 'Mismatch']
    try:
        harvest_filepath = processingRecord.get('harvest_filepath', None)
        recBibcode = processingRecord.get('bibcode', None)
        master_doi = processingRecord.get('master_doi', None)
        if processingRecord.get('record', None) == '':
            status = 'NoIndex'
            matchtype = 'Other'
            classic_match = {}
            classic_bibcode = None
        else:
            xmatchResult = xmatch.match(master_doi, recBibcode)
            matchtype = xmatchResult.get('match', None)
            if matchtype in allowedMatchType:
                status = 'Matched'
            else:
                status = 'Unmatched'
            if matchtype == 'Classic Canonical Bibcode':
                matchtype = 'Other'
            classic_match = xmatchResult.get('errs', {})
            classic_bibcode = xmatchResult.get('bibcode', None)
    except Exception as err:
        logger.warning("Error matching record: %s" % err)
    else:
        try:
            if type(classic_match) == dict:
                classic_match = json.dumps(classic_match)
            issns = processingRecord.get('issns', {})
            if type(issns) == dict:
                issns = json.dumps(issns)
            master_bibdata = processingRecord.get('master_bibdata', {})
            if type(master_bibdata) == dict:
                master_bibdata = json.dumps(master_bibdata)
            outputRecord = (harvest_filepath,
                            master_doi,
                            issns,
                            master_bibdata,
                            classic_match,
                            status,
                            matchtype,
                            recBibcode,
                            classic_bibcode)
            task_write_result_to_db.delay(outputRecord)
        except Exception as err:
            logger.warning("Error creating a master record: %s" % err)

@app.task(queue='add-bibcode')
def task_add_bibcode(processingRecord):
    try:
        record = processingRecord.get('record', None)
        bibcode = bibgen.make_bibcode(record)
    except Exception as err:
        logger.info("Failed to create bibcode: %s" % err)
        bibcode = None
    processingRecord['bibcode'] = bibcode
    task_match_record_to_classic.delay(processingRecord)

@app.task(queue='parse-meta')
def task_add_empty_record(infile):
    try:
        (doi, issns) = utils.simple_parse_one_meta_xml(infile)
        bib_data = {}
        processingRecord = {'record': '',
                            'harvest_filepath': infile,
                            'master_doi': doi,
                            'issns': json.dumps(issns),
                            'master_bibdata': json.dumps(bib_data)}
        task_add_bibcode.delay(processingRecord)
    except Exception as err:
        raise EmptyRecordException(err)

@app.task(queue='parse-meta')
def task_process_metafile(infile):
    try:
        record = utils.parse_one_meta_xml(infile)
        publication = record.get('publication', None)
        pagination = record.get('pagination', None)
        pids = record.get('persistentIDs', None)
        first_author = record.get('authors', None)
        title = record.get('title', None)
        if publication:
            pub_year = publication.get('pubYear', None)
            issns = publication.get('ISSN', None)
        if pids:
            doi = None
            for pid in pids:
                if pid.get('DOI', None):
                    doi = pid.get('DOI', None)
        if not doi:
            logger.error("Unable to extract DOI from record: %s" % infile)
        if first_author:
            first_author = first_author[0]
    except Exception as err:
        logger.info("Unable to process metafile %s, logging without bibdata" % infile)
        try:
            task_add_empty_record.delay(infile)
        except Exception as err:
            logger.warning("Record %s failed, won't be written to db: %s" % (infile, err))
#ADD ME
        # task_write_result_to_db({DOI, filename, 'NotIndexed'})
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
        task_add_bibcode.delay(processingRecord)

@app.task(queue='get-logfiles')
def task_process_logfile(infile):
    try:
        files_to_process = utils.read_updateagent_log(infile)
        for xmlFile in files_to_process:
            xmlFile = app.conf.get('HARVEST_BASE_DIR', '/') + xmlFile
            task_process_metafile.delay(xmlFile)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))
