import json
import math
import os

from sqlalchemy import func

from adscompstat import app as app_module
from adscompstat.exceptions import BibstemLookupException, FetchClassicBibException
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
logger = app.logger


class DataBaseSession(object):

    def __init__(self):
        self.session = app.session_scope()

    def _clear_classic_data(self):
        try:
            self.session.query(identifier_doi).delete()
            self.session.query(alt_identifiers).delete()
            self.session.query(issn_bibstem).delete()
            self.session.commit()
            logger.info("Existing classic data tables cleared.")

    def _query_master_by_doi(self, doi):
        return self.session.query(master.master_doi).filter_by(master_doi=doi).all()

    def _update_master_by_doi(self, row_modeldict):
        doi = row_modeldict.get("master_doi", None)
        try:
            self.session.query(master).filter_by(master_doi=doi).update(row_modeldict)
            self.session.commit()
        except Exception as err:
            session.rollback()
            session.flush()
            logger.warning("DB write error: %s; Record: %s" % (err, record))

    def _query_bibstem_by_issn(self, issn):
        try:
            results = self.session.query(issn_bibstem.bibstem).filter(issn_bibstem.issn == issnString).first()
            return results
        except Exception as err:
            logger.warning("Unable to get bibstem from issn %s: %s" % (issn, err))
        
    def _query_completeness_per_bibstem(self, bibstem):
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
            logger.warning("Error querying completeness for bibstem %s: %s" % (bibstem, err))


    def _query_classic_bibcodes(self, doi, bibcode):
        bibcodesFromDoi = []
        bibcodesFromBib = []
        try:
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
        except Exception as err:
            raise FetchClassicBibException(err)
        else:
            return bibcodesFromDoi, bibcodesFromBib
      
    def _write_completeness_summary(self, summary):
        try:
            self.session.add(summary)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            logger.warning(
                "Error writing summary data for %s, v %s: %s" % (bibstem, k, err)
            )

    def _query_retry_files(self, rec_type):
        try:
            result = self.session.query(master.harvest_filepath).filter(master.matchtype == rec_type).all()
            )
            return result
        except Exception as err:
            logger.error("Unable to retrieve retry files of type %s: %s" % (rec_type, err))

    def _query_summary_bibstems(self):
        try:
            bibstems = self.session.query(summary.bibstem).distinct().all()
            bibstems = [x[0] for x in bibstems]
            return bibstems
        except Exception as err:
            logger.error("Failed to get bibstems from summary: %s" % err)

    def _query_summary_single_bibstem(self, bibstem):
        try:
            result = self.session.query(
                    summary.bibstem,
                    summary.volume,
                    summary.complete_fraction,
                    summary.paper_count,
                )
                .filter(summary.bibstem == bibstem)
                .all()
            )
            return result
        except Exception as err:
            logger.error("Failed to get completeness for bibstem %s: %s" % (bibstem, err))

    def _query_unique_bibstems(self):
        try:
            bibstems = self.session.query(func.substr(master.bibcode_meta, 5, 5)).distinct().all()
            return bibstems
        except Exception as err:
            logger.warning("No bibstems from master: %s" % err)

    def _delete_previous_summary(self):
        try:
            self.session.query(summary).delete()
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            logger.error("Unable to clear summary table: %s" % err)

    def _write_block(self, table, datablock):
        try:
            self.session.bulk_insert_mappings(table, datablock)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            self.session.flush()
            logger.warning("Failed to write data block: %s" % err)
