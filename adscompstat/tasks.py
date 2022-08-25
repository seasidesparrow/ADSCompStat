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
        print('oh noes! %s' % err)
    else:
        bib_data = {'publication': publication,
                   'pagination': pagination,
                   'persistentIDs': pids,
                   'first_author': first_author,
                   'title': title}
        source_file = infile
        output_rec = (doi, json.dumps(issns), json.dumps(bib_data), source_file)
        task_add_bibcode(record, output_rec)

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

@app.task(queue='match-classic')
def task_match_record_to_classic(outputRecord):
    recDOI = outputRecord.get('doi', None)
    recBibcode = outputRecord.get('bibcode', None)
    outputRecord['result'] = xmatch.match(recDOI, recBibcode)
    task_write_result_to_db(outputRecord)

@app.task(queue='parse-meta')
def task_process_xref_xml(infile):
    try:
        filename = config.get('HARVEST_BASE_DIR') + '/' + infile
        record = utils.parse_one_meta_xml(filename)
        if record:
            try:
                doi = None
                idlist = record.get('persistentIDs', None)
                for i in idlist:
                    doi = i.get('DOI', None)
                if doi:
                    try:
                        bibcode = bibgen.make_bibcode(record)
                    except Exception as err:
                        bibcode = None
# TASK: add delay
                    return (doi, bibcode, xmatch.match(doi, bibcode))
                else:
                    raise RecordException('No doi from record')
            except Exception as err:
                raise RecordException(err)
        else:
            raise ParseMetaXMLException('Null record from file parser.')
    except Exception as err:
        raise GetMetaException(err)


def task_process_logfile(infile):
    files_to_process = utils.read_updateagent_log(infile)
    output_records = list()
    for xmlFile in files_to_process:
        try:
# TASK: add delay
            (doi, xrbibcode, result) = task_process_xref_xml(xmlFile)
            outstring = "%s\t%s\t%s\t%s" % (xrbibcode, doi, result, xmlFile)
            # print('%s\t%s\t%s\t%s' % (xrbibcode, doi, result, xmlFile))
            output_records.append(outstring)
        except Exception as err:
            # logger.warn('error processing logfile: %s' % err)
            # print('tasks.task_process_logfile: error processing xmlFile %s: %s' % (xmlFile, err))
            outstring = "tasks.task_process_logfile: error processing xmlFile %s: %s" % (xmlFile, err)
            output_records.append(outstring)
    return output_records
