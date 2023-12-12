try:
    from adsputils import UTCDateTime, get_date
except ImportError:
    from adsmutils import get_date, UTCDateTime

from sqlalchemy import Column, Float, Integer, String, Text
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CompStatMaster(Base):
    __tablename__ = "master"

    match_status = ENUM("Matched", "Unmatched", "NoIndex", "Failed", name="match_status")
    match_type = ENUM(
        "canonical",
        "deleted",
        "alternate",
        "partial",
        "mismatch",
        "unmatched",
        "other",
        "failed",
        name="match_type",
    )

    masterid = Column(Integer, primary_key=True, unique=True)
    harvest_filepath = Column(String, nullable=False)
    master_doi = Column(String, unique=True, nullable=False)
    issns = Column(Text, nullable=True)
    db_origin = Column(String, nullable=False)
    master_bibdata = Column(Text, nullable=False)
    classic_match = Column(Text, nullable=True)
    status = Column(match_status, nullable=False)
    matchtype = Column(match_type, nullable=False)
    bibcode_meta = Column(String, nullable=True)
    bibcode_classic = Column(String, nullable=True)
    notes = Column(String, nullable=True)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)

    def __repr__(self):
        return "master.masterid='{self.masterid}', master.db_origin='{self.db_origin}', master.master_doi='{self.master_doi}'".format(
            self=self
        )

    def toJSON(self):
        return {
            "masterid": self.masterid,
            "harvest_filepath": self.harvest_filepath,
            "master_doi": self.master_doi,
            "issns": self.issns,
            "db_origin": self.db_origin,
            "master_bibdata": self.master_bibdata,
            "classic_match": self.classic_match,
            "status": self.status,
            "matchtype": self.matchtype,
            "bibcode_meta": self.bibcode_meta,
            "bibcode_classic": self.bibcode_classic,
            "notes": self.notes,
            "created": self.created,
            "updated": self.updated,
        }


class CompStatSummary(Base):
    __tablename__ = "summary"

    summaryid = Column(Integer, primary_key=True, unique=True)
    bibstem = Column(String, nullable=False)
    volume = Column(String, nullable=False)
    paper_count = Column(Integer, nullable=False)
    complete_fraction = Column(Float, nullable=True)
    complete_details = Column(Text, nullable=True)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)

    def __repr__(self):
        return "summary.summaryid='{self.summaryid}', summary.complete_fraction='{self.summary.complete_fraction}'"

    def toJSON(self):
        return {
            "summaryid": self.summaryid,
            "bibstem": self.bibstem,
            "volume": self.volume,
            "paper_count": self.paper_count,
            "complete_fraction": self.complete_fraction,
            "complete_details": self.complete_details,
            "created": self.created,
            "updated": self.updated,
        }


class CompStatIdentDoi(Base):
    __tablename__ = "identifier_doi"

    identifier = Column(String, primary_key=True, nullable=False)
    doi = Column(String, primary_key=True, unique=True, nullable=False)

    def __repr__(self):
        return "identifier_doi.identifier='{self.identifier}', identifier_doi.doi='{self.doi}'"


class CompStatIssnBibstem(Base):
    __tablename__ = "issn_bibstem"

    bibstem = Column(String, primary_key=True, nullable=False)
    issn = Column(String, primary_key=True, unique=True, nullable=False)
    issn_type = Column(String, nullable=False)

    def __repr__(self):
        return "issn_bibstem.bibstem='{self.bibstem}', issn_bibstem.issn='{self.issn}', issn_bibstem.issn_type='{self.issn_type}'"


class CompStatAltIdents(Base):
    __tablename__ = "alt_identifiers"

    identifier = Column(String, primary_key=True, unique=True, nullable=False)
    canonical_id = Column(String, primary_key=True, unique=False, nullable=False)
    idtype = Column(String, unique=False, nullable=False)

    def __repr__(self):
        return "alt_identifiers.identifier='{self.identifier}', alt_identifiers.canonical_id='{self.canonical_id}', alt_identifiers.idtype='{self.idtype}'"
