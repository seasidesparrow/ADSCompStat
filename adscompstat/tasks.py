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

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCompStatCelery('compstat-pipeline', proj_home=proj_home, config=globals().get('config', {}), local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('parse-metafile', app.exchange, routing_key='parse-metafile'),
    Queue('add-emptyrecord', app.exchange, routing_key='add-emptyrecord'),
    Queue('compute-stats', app.exchange, routing_key='compute-stats'),
    Queue('output-metadata', app.exchange, routing_key='output-metadata')
)

@app.task(queue='output-metadata')
def task_write_result_to_master(inrec):
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
                logger.debug("Record for DOI %s exists already, ignoring for now." % checkdoi)
        except Exception as err:
            session.rollback()
            session.flush()
            logger.warning("Problem with database commit: %s" % err)

@app.task(queue='add-emptyrecord')
def task_add_empty_record(infile):
    try:
        (doi, issns) = utils.simple_parse_one_meta_xml(infile)
        issn_dict={}
        if issns:
            for item in issns:
                k = item[0]
                v = item[1]
                issn_dict[k] = v
        issn_dict = json.dumps(issn_dict)
        db_origin = 'Crossref'
        bib_data = json.dumps({})
        classic_match = json.dumps({})
        status = 'NoIndex'
        matchtype = 'other'
        bibcode_meta = ''
        bibcode_classic = ''
        outrec=(infile, doi, issn_dict, bib_data, classic_match,
                status, matchtype, bibcode_meta, bibcode_classic)
        task_write_result_to_master.delay(outrec)
    except Exception as err:
        logger.warning("Can't add empty record for %s: %s" % (infile, err))

@app.task(queue='parse-metafile')
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
            issn_dict={}
            if issns:
                for item in issns:
                    k = item['pubtype']
                    v = item['issnString']
                    if len(v) == 8:
                        v = v[0:4]+'-'+v[4:]
                    issn_dict[k] = v
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
        logger.debug("Unable to process metafile %s, logging without bibdata" % infile)
        try:
            task_add_empty_record.delay(infile)
        except Exception as err:
            logger.warning("Record %s failed, won't be written to db: %s" % (infile, err))
    else:
        bib_data = {'publication': publication,
                   'pagination': pagination,
                   'persistentIDs': pids,
                   'first_author': first_author,
                   'title': title}
        processingRecord = {'record': record,
                            'harvest_filepath': infile,
                            'master_doi': doi,
                            'issns': issn_dict,
                            'master_bibdata': bib_data}
        try:
            bibstems = list()
            with app.session_scope() as session:
                for issn in list(set(issn_dict.values())):
                    result = session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn==issn).all()
                    for r in result:
                        bibstems.append(r[0])

            bibstems = list(set(bibstems))
            if len(bibstems) == 0:
                processingRecord['bibcode'] = None
                logger.warning("No bibstems found for issn(s) %s!" % issn_dict)
            elif len(bibstems) > 1:
                processingRecord['bibcode'] = None
                logger.warning("Multiple bibstems found for issn(s) %s!" % issn_dict)
            else:
                bibgen = BibcodeGenerator(bibstem=bibstems[0], issn2bibstem={}, name2bibstem={})
                processingRecord['bibcode'] = bibgen.make_bibcode(record)
        except Exception as err:
            logger.debug("Failed to create bibcode: %s" % err)
            processingRecord['bibcode'] = None

        bibcodesFromXDoi = []
        bibcodesFromXBib = []
        with app.session_scope() as session:
            bibcodesFromXDoi = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).join(identifier_doi, alt_identifiers.canonical_id == identifier_doi.identifier).filter(identifier_doi.doi == doi).all()
            if processingRecord.get('bibcode', None):
                bibcodesFromXBib = session.query(alt_identifiers.identifier, alt_identifiers.canonical_id, alt_identifiers.idtype).filter(alt_identifiers.identifier == processingRecord['bibcode']).all()

        xmatch = CrossrefMatcher()
        xmatchResult = xmatch.match(processingRecord['bibcode'], bibcodesFromXDoi, bibcodesFromXBib)
        if xmatchResult:
            matchtype = xmatchResult.get('match', None)
            if matchtype in ['canonical', 'deleted', 'alternate', 'partial', 'other', 'mismatch']:
                status = 'Matched'
            else:
                status = 'Unmatched'
            if matchtype == 'Classic Canonical Bibcode':
                matchtype = 'other'
            classic_match = xmatchResult.get('errs', {})
            classic_bibcode = xmatchResult.get('bibcode', None)
        else:
            status='NoIndex'
            matchtype = 'other'
            classic_match = {}
            classic_bibcode = None
        try:
            harvest_filepath = processingRecord.get('harvest_filepath', None)
            recBibcode = processingRecord.get('bibcode', None)
            master_doi = processingRecord.get('master_doi', None)
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
            task_write_result_to_master.delay(outputRecord)
        except Exception as err:
            logger.warning("Error creating a master record, record not added to database: %s" % err)

def task_do_all_completeness():
    try:
        with app.session_scope() as session:
            bibstems = session.query(func.substr(master.bibcode_meta,5,5)).distinct().all()
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

@app.task(queue='compute-stats')
def task_completeness_per_bibstem(bibstem):
    if len(bibstem) < 5:
        bibstem = bibstem.ljust(5, ".")
    try:
        with app.session_scope() as session:
            result = session.query(func.substr(master.bibcode_meta, 5, 10),master.status,master.matchtype,func.count(master.bibcode_meta)).filter(func.substr(master.bibcode_meta, 5, 5)==bibstem).group_by(func.substr(master.bibcode_meta, 5, 10), master.status, master.matchtype).all()
    except Exception as err:
        logger.warning("Failed to get completeness summary for bibstem %s: %s" % (bibstem, err))
    else:
        # result is an array of tuples with (bibstem+vol,status,matchtype,count)
        # volumes = list(set([x[0][5:] for x in result]))
        # volumes.sort()
        volumeSummary = dict()
        for r in result:
            vol = r[0][5:].lstrip('.').rstrip('.')
            stat = r[1]
            mtype = r[2]
            count = r[3]
            if volumeSummary.get(vol, None):
                volumeSummary[vol].append({'status': stat, 'matchtype': mtype, 'count': count})
            else:
                volumeSummary[vol] = [{'status': stat, 'matchtype': mtype, 'count': count}]
        for k, v in volumeSummary.items():
            try:
                (countrecs, compfrac) = utils.get_completeness_fraction(v)
                outrec = summary(bibstem=bibstem.rstrip('.'),
                                 volume = k,
                                 paper_count=countrecs,
                                 complete_fraction = compfrac,
                                 complete_details = json.dumps(v))
            except Exception as err:
                logger.warning("Error calculating summary completeness data for %s, v %s: %s" % (bibstem,k,err))
            else:
                with app.session_scope() as session:
                    try:
                        session.add(outrec)
                        session.commit()
                    except Exception as err:
                        session.rollback()
                        session.flush()
                        logger.warning("Error writing summary data for %s, v %s: %s" % (bibstem,k,err))


def task_process_logfile(infile):
    try:
        files_to_process = utils.read_updateagent_log(infile)
        for xmlFile in files_to_process:
            xmlFile = app.conf.get('HARVEST_BASE_DIR', '/') + xmlFile
            task_process_metafile.delay(xmlFile)
    except Exception as err:
        logger.error("Error processing logfile %s: %s" % (infile, err))


def task_clear_classic_data():
    with app.session_scope() as session:
        try:
            session.query(identifier_doi).delete()
            session.query(alt_identifiers).delete()
            session.query(issn_bibstem).delete()
            session.commit()
        except Exception as err:
            session.rollback()
            session.commit()
            logger.error("Failed to clear classic data tables: %s" % err)


def task_export_completeness_to_json():
    with app.session_scope() as session:
        try:
            allData = []
            bibstems = session.query(summary.bibstem).distinct().all()
            bibstems = [x[0] for x in bibstems]
            for bib in bibstems:
                completeness = []
                result = session.query(summary.bibstem,summary.volume,summary.complete_fraction,summary.paper_count).filter(summary.bibstem==bib).all()
                paperCount = 0
                averageCompleteness = 0.0
                for r in result:
                    completeness.append({'volume': r[1],
                                         'completeness': r[2]})
                    paperCount += r[3]
                    averageCompleteness += r[3]*r[2]
                averageCompleteness = averageCompleteness / paperCount
                allData.append({'bibstem': bib,
                                'completeness_fraction': averageCompleteness,
                                'completeness_details': completeness})
            if allData:
                utils.export_completeness_data(allData, app.conf.get('COMPLETENESS_EXPORT_FILE', None))
        except Exception as err:
            logger.error("Unable to export completeness data to disk: %s" % err)
                
                

def task_load_classic_data():
    # You have three tables to load....
    blocksize = 100000

    # Clear the existing tables in postgres
    try:
        task_clear_classic_data()
    except Exception as err:
        logger.error("Failed to clear classic data tables: %s" % err)
    else:

        # table 1: canonical id versus DOI writes to table identifier_doi
        try:
            records = utils.load_classic_doi_bib_dict(app.conf.get('CLASSIC_DOI_FILE', None))
        except Exception as err:
            logger.error("Couldn't read canonical-doi mapping file: %s" % err)
        else:
            try:
                with app.session_scope() as session:
                    insertlist = []
                    for doi, bibc in records.items():
                        rec = {'doi': doi, 'identifier': bibc}
                        insertlist.append(rec)
                    try:
                        while insertlist:
                            insertblock = insertlist[0:blocksize]
                            insertlist = insertlist[blocksize:]
                            session.bulk_insert_mappings(identifier_doi, insertblock)
                            session.commit()
                    except Exception as err:
                        session.rollback()
                        session.flush()
                        logger.warning("Error loading record block: %s" % err)
            except Exception as err:
                logger.error("Error loading doi-canonical bibcode mapping: %s" % err)

        # table 2: canonical and alternate / deleted / other identifiers
        # merge the data first in utils.merge_alternate_bibcodes
        try:
            records = utils.merge_bibcode_lists(app.conf.get('CLASSIC_CANONICAL', None), app.conf.get('CLASSIC_ALTBIBS', None), app.conf.get('CLASSIC_DELBIBS', None), app.conf.get('CLASSIC_ALLBIBS', None))
        except Exception as err:
            logger.error("Couldn't read canonical-doi mapping files: %s" % err)
        else:
            try:
                with app.session_scope() as session:
                    insertlist = []
                    for k, v in records.items():
                        ident = k
                        canid = v.get('canonical_id', None)
                        idtype = v.get('idtype', None)
                        if canid and idtype:
                            rec = {'identifier': ident,
                                   'canonical_id': canid,
                                   'idtype': idtype}
                            insertlist.append(rec)
                    try:
                        while insertlist:
                            insertblock = insertlist[0:blocksize]
                            insertlist = insertlist[blocksize:]
                            session.bulk_insert_mappings(alt_identifiers, insertblock)
                            session.commit()
                    except Exception as err:
                        session.rollback()
                        session.flush()
                        logger.warning("Error loading record block: %s" % err)
            except Exception as err:
                logger.error("Error loading alt-canonical bibcode mapping: %s" % err)

        # table 3: bibstem to issn mapping
        try:
            issnrecs = utils.load_journalsdb_issn_bibstem_list(app.conf.get('JOURNALSDB_ISSN_BIBSTEM', None))
        except Exception as err:
            logger.error("Error reading bibstem-issn mapping: %s" % err)
        else:
            with app.session_scope() as session:
                try:
                    session.bulk_insert_mappings(issn_bibstem, issnrecs)
                    session.commit()
                except Exception as err:
                    session.rollback()
                    session.flush()
                    logger.error("Error loading bibstem-issn mapping: %s" % err)

