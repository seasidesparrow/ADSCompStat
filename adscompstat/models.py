try:
    from adsputils import get_date, UTCDateTime
except ImportError:
    from adsmutils import get_date, UTCDateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Table, Column, Integer, Numeric, String, TIMESTAMP,
                        ForeignKey, Boolean, Float, Text, UniqueConstraint)
from sqlalchemy.dialects.postgresql import ENUM

Base = declarative_base()


class CompStatMaster(Base):
    __tablename__ = 'master'

    match_status = ENUM('Matched', 'Unmatched', 'NoIndex', name='match_status')
    match_type = ENUM('Exact', 'Deleted', 'Alternate', 'Partial', 'Mismatch', 'Unmatched', 'Other', name='match_type')

    masterid = Column(Integer, primary_key=True, unique=True)
    harvest_filepath = Column(String, nullable=False)
    master_doi = Column(String, unique=True, nullable=False)
    issns = Column(Text, nullable=True)
    db_origin = Column(String, nullable=False)
    master_bibdata = Column(Text, nullable=False)
    classic_match = Column(Text, nullable=True)
    status = Column(match_status, nullable=False)
    matchtype = Column(match_type, nullable=False)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)

    def __repr__(self):
        return "master.masterid='{self.masterid}', master.db_origin='{self.db_origin}', master.master_doi='{self.master_doi}'".format(self=self)

    def toJSON(self):
        return {'masterid': self.masterid,
                'harvest_filepath': self.harvest_filepath,
                'master_doi': self.master_doi,
                'issns': self.issns,
                'db_origin': self.db_origin,
                'master_bibdata': self.master_bibdata,
                'classic_match': self.classic_match,
                'status': self.status,
                'matchtype': self.matchtype,
                'updated': self.updated}


class CompStatSummary(Base):
    __tablename__ = 'summary'

    summaryid = Column(Integer, primary_key=True, unique=True)
    bibstem = Column(String, nullable=False)
    complete_flag = Column(Boolean, nullable=True)
    complete_fraction = Column(Float, nullable=True)
    complete_byvolume = Column(Text, nullable=True)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, onupdate=get_date)

    def __repr__(self):
        return "summary.summaryid='{self.summaryid}', summary.complete_fraction='{self.summary.complete_fraction}'"

    def toJSON(self):
        return {'summaryid': self.summaryid,
                'bibstem': self.bibstem,
                'complete_flag': self.complete_flag,
                'complete_fraction': self.complete_fraction,
                'complete_byvolume': self.complete_byvolume,
                'updated': self.updated}
