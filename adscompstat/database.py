import json
import os

from sqlalchemy import func

from adscompstat import app as app_module
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatIssnBibstem as issn_bibstem
from adscompstat.models import CompStatMaster as master
from adscompstat.models import CompStatSummary as summary
from adsputils import load_config, setup_logging

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
config = load_config(proj_home=proj_home)
logger = setup_logging(__name__, proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

class DBClearClassicException(Exception):
    pass


class DBClearSummaryException(Exception):
    pass


class DBWriteException(Exception):
    pass


class DBQueryException(Exception):
    pass


def clear_classic_data(app):
    with app.session_scope() as session:
        try:
            session.query(identifier_doi).delete()
            session.query(alt_identifiers).delete()
            session.query(issn_bibstem).delete()
            session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBClearClassicException("Existing classic data tables not cleared: %s" % err)

def clear_summary_data(app):
    with app.session_scope() as session:
        try:
            session.query(summary).delete()
            session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBClearSummaryException("Failed to clear summary table: %s" % err)

def query_master_by_doi(app, doi):
    with app.session_scope() as session:
        try:
            return session.query(master.master_doi).filter_by(master_doi=doi).all()
        except Exception as err:
            raise DBQueryException("Unable to query master by DOI %s: %s" % (doi, err))

def query_bibstem_by_issn(app, issn):
    with app.session_scope() as session:
        try:
            return session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn == issn).first()
        except Exception as err:
            raise DBQueryException("Unable to get bibstem from issn %s: %s" % (issn, err))

def query_completeness_per_bibstem(app, bibstem):
    with app.session_scope() as session:
        try:
            result = (
                session.query(
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
            raise DBQueryException("Error querying completeness for bibstem %s: %s" % (bibstem, err))

def query_classic_bibcodes(app, doi, bibcode):
    with app.session_scope() as session:
        bibcodesFromDoi = []
        bibcodesFromBib = []
        try:
            if doi:
                bibcodesFromDoi = (
                    session.query(
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
                    session.query(
                        alt_identifiers.identifier,
                        alt_identifiers.canonical_id,
                        alt_identifiers.idtype,
                    )
                    .filter(alt_identifiers.identifier == bibcode)
                    .all()
                )
            return bibcodesFromDoi, bibcodesFromBib
        except Exception as err:
            raise DBQueryException(err)

def query_retry_files(app, rec_type):
    with app.session_scope() as session:
        try:
            return session.query(master.harvest_filepath).filter(master.matchtype == rec_type).all()
        except Exception as err:
            raise DBQueryException("Unable to retrieve retry files of type %s: %s" % (rec_type, err))

def query_master_bibstems(app):
    with app.session_scope() as session:
        try:
            return session.query(func.substr(master.bibcode_meta, 5, 5)).distinct().all()
        except Exception as err:
            raise DBQueryException("Failed to get unique bibstems from master: %s" % err)

def query_summary_bibstems(app):
    with app.session_scope() as session:
        try:
            bibstems = session.query(summary.bibstem).distinct().all()
            bibstems = [x[0] for x in bibstems]
            return bibstems
        except Exception as err:
            raise DBQueryException("Failed to get bibstems from summary: %s" % err)

def query_summary_single_bibstem(app, bibstem):
    with app.session_scope() as session:
        try:
            result = session.query(
                    summary.bibstem,
                    summary.volume,
                    summary.complete_fraction,
                    summary.paper_count,
                    ).filter(summary.bibstem == bibstem).all()
            return result
        except Exception as err:
            raise DBQueryException("Failed to get completeness for bibstem %s: %s" % (bibstem, err))

def update_master_by_doi(app, update):
    with app.session_scope() as session:
        try:
            doi = update.get("master_doi", None)
            session.query(master).filter_by(master_doi=doi).update(update)
            session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBWriteException("Error writing record to master: %s; row data: %s" % (err, update))

def write_completeness_summary(app, summary):
    with app.session_scope() as session:
        try:
            session.add(summary)
            session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBWriteException("Error writing summary data: %s" % err)

def write_block(app, table, datablock):
    with app.session_scope() as session:
        try:
            session.bulk_insert_mappings(table, datablock)
            session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBWriteException("Failed to bulk write data block: %s" % err)

def write_matched_record(app, result, record):
    with app.session_scope() as session:
        try:
            if result:
                update = {
                    "harvest_filepath": record[0],
                    "master_doi": record[1],
                    "issns": record[2],
                    "db_origin": "Crossref",
                    "master_bibdata": record[3],
                    "classic_match": record[4],
                    "status": record[5],
                    "matchtype": record[6],
                    "bibcode_meta": record[7],
                    "bibcode_classic": record[8],
                    "notes": record[9],
                }
                update_master_by_doi(app, update)
            else:
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
                session.add(row)
                session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            raise DBWriteException("Failed to add/update row in master: %s" % err)
