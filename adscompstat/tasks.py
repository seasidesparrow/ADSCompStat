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
def task_write_result_to_db(outputRecord):
    with app.session_scope as session:
        try:
            session.add(outputRecord)
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
        if matchtype in allowedMatchType:
            status = 'Matched'
        classic_match = xmatchResult.get('errs', None)
        if classic_match:
            classic_match = json.dumps(classic_match)
        outputRecord = master(harvest_filepath=processingRecord.get('harvest_filepath', None),
                              master_doi=processingRecord.get('master_doi', None),
                              issns=processingRecord.get('issns', None),
                              master_bibdata=processingRecord.get('master_bibdata', None),
                              classic_match=classic_match,
                              status=status,
                              matchtype=matchtype)
        task_write_result_to_db(outputRecord)
    except Exception as err:
        logger.warn("Error matching record")

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
        task_match_record_to_classic(outputRecord)

@app.task(queue='parse-meta')
def task_process_metafile(infile)
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
                            'issns': json.dumps(bib_data),
                            'master_bibdata': json.dumps(bib_data)}
        task_match_record_to_classic.delay(processingRecord)
