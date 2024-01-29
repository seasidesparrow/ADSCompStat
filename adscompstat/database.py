import json
import os

from sqlalchemy import func

from adscompstat import app as app_module
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatIssnBibstem as issn_bibstem
from adscompstat.models import CompStatMaster as master
from adscompstat.models import CompStatSummary as summary

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
app = app_module.ADSCompStatCelery(
    "completeness-statistics-pipeline",
    proj_home=proj_home,
    config=globals().get("config", {}),
    local_config=globals().get("local_config", {}),
)

class DBClearClassicException(Exception):
    pass


class DBClearSummaryException(Exception):
    pass


class DBWriteException(Exception):
    pass


class DBQueryException(Exception):
    pass


class DataBaseSession(object):

    def _init__(self):
        self.session = app.session_scope()

    def clear_classic_data(self):
        try:
            self.session.query(identifier_doi).delete()
            self.session.query(alt_identifiers).delete()
            self.session.query(issn_bibstem).delete()
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBClearClassicException("Existing classic data tables not cleared: %s" % err)

    def clear_summary_data(self):
        try:
            self.session.query(summary).delete()
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBClearSummaryException("Failed to clear summary table: %s" % err)

    def query_master_by_doi(self, doi):
        try:
            return self.session.query(master.master_doi).filter_by(master_doi=doi).all()
        except Exception as err:
            raise DBQueryException("Unable to query master by DOI %s: %s" % (doi, err))

    def query_bibstem_by_issn(self, issn):
        try:
            return self.session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn == issnString).first()
        except Exception as err:
            raise DBQueryException("Unable to get bibstem from issn %s: %s" % (issn, err))
        
    def query_completeness_per_bibstem(self, bibstem):
        try:
            result = (
                self.session.query(
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

    def query_classic_bibcodes(self, doi, bibcode):
        bibcodesFromDoi = []
        bibcodesFromBib = []
        try:
            if doi:
                bibcodesFromDoi = (
                    self.session.query(
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
                    self.session.query(
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

    def query_retry_files(self, rec_type):
        try:
            return self.session.query(master.harvest_filepath).filter(master.matchtype == rec_type).all()
        except Exception as err:
            raise DBQueryException("Unable to retrieve retry files of type %s: %s" % (rec_type, err))

    def query_master_bibstems(self):
        try:
            return self.session.query(func.substr(master.bibcode_meta, 5, 5)).distinct().all()
        except Exception as err:
            raise DBQueryException("Failed to get unique bibstems from master: %s" % err)

    def query_summary_bibstems(self):
        try:
            bibstems = self.session.query(summary.bibstem).distinct().all()
            bibstems = [x[0] for x in bibstems]
            return bibstems
        except Exception as err:
            raise DBQueryException("Failed to get bibstems from summary: %s" % err)

    def query_summary_single_bibstem(self, bibstem):
        try:
            result = self.session.query(
                    summary.bibstem,
                    summary.volume,
                    summary.complete_fraction,
                    summary.paper_count,
                    ).filter(summary.bibstem == bibstem).all()
            return result
        except Exception as err:
            raise DBQueryException("Failed to get completeness for bibstem %s: %s" % (bibstem, err))

    def update_master_by_doi(self, row_modeldict):
        try:
            doi = row_modeldict.get("master_doi", None)
            self.session.query(master).filter_by(master_doi=doi).update(row_modeldict)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBWriteException("Error writing record to master: %s; row data: %s" % (err, row_modeldict))
      
    def write_completeness_summary(self, summary):
        try:
            self.session.add(summary)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBWriteException("Error writing summary data: %s" % err)

    def write_block(self, table, datablock):
        try:
            self.session.bulk_insert_mappings(table, datablock)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBWriteException("Failed to bulk write data block: %s" % err)

    def write_matched_record(self, result, record):
        try:
            if result:
                self.update_master_by_doi(record)
            else:
                self.session.add(record)
                self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            raise DBWriteException("Failed to add/update row in master: %s" % err)
