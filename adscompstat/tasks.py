#from __future__ import absolute_import, unicode_literals
from config import *

#from adscompstat import app as app_module
from adscompstat import utils
from adscompstat.bibcodes import BibcodeGenerator
from adscompstat.match import CrossrefMatcher
from adscompstat.exceptions import *

import os
#from kombu import Queue
#from sqlalchemy import exc

from datetime import datetime

# ============================= INITIALIZATION ==================================== #

#proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
#app = app_module.ADSCompletenessCelery('journals-database', proj_home=proj_home, local_config=globals().get('local_config', {}))
#logger = app.logger


#app.conf.CELERY_QUEUES = (
#    Queue('do-completeness', app.exchange, routing_key='do-completeness'),
#)

# ============================= TASKS ============================================= #


try:
    xmatch = CrossrefMatcher()
    bibgen = BibcodeGenerator()
except Exception as err:
    raise NoDataHandlerException(err)


#@app.task(queue='do-completeness')
def task_process_xref_xml(infile):
    try:
        filename = HARVEST_BASE_DIR + '/' + infile
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
