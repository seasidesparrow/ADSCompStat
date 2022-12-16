"""Add storage tables

Revision ID: f0123456789a
Revises: 451c42f1578d
Create Date: 2022-08-15 12:00:00.000000

"""
from alembic import op
from adsputils import UTCDateTime, get_date
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f0123456789a'
down_revision = '451c42f1578d'
branch_labels = None
depends_on = None


def upgrade():
    # master record for each doi
    op.create_table('master',
                    sa.Column('masterid', sa.Integer(), autoincrement=True,
                              nullable=False),
                    sa.Column('harvest_filepath', sa.String(), nullable=False),
                    sa.Column('master_doi', sa.String(), nullable=False),
                    sa.Column('issns', sa.Text(), nullable=True),
                    sa.Column('db_origin', sa.String(), nullable=False),
                    sa.Column('master_bibdata', sa.Text(), nullable=False),
                    sa.Column('classic_match', sa.Text(), nullable=True),
                    sa.Column('status', postgresql.ENUM('Matched',
                                                        'Unmatched',
                                                        'NoIndex',
                                                        name='match_status'),
                              nullable=False),
                    sa.Column('matchtype', postgresql.ENUM('canonical',
                                                           'deleted',
                                                           'alternate',
                                                           'partial',
                                                           'mismatch',
                                                           'unmatched',
                                                           'other',
                                                           name='match_type'),
                              nullable=False),
                    sa.Column('bibcode_meta', sa.String(), nullable=True),
                    sa.Column('bibcode_classic', sa.String(), nullable=True),
                    sa.Column('created', UTCDateTime, nullable=True,
                              default=get_date),
                    sa.Column('updated', UTCDateTime, nullable=True,
                              onupdate=get_date),
                    sa.PrimaryKeyConstraint('masterid'),
                    sa.UniqueConstraint('master_doi'),
                    sa.UniqueConstraint('masterid'))

    # summary record for each bibstem, per volume
    op.create_table('summary',
                    sa.Column('summaryid', sa.Integer(), autoincrement=True,
                              nullable=False),
                    sa.Column('bibstem', sa.String(),
                              nullable=False),
                    sa.Column('volume', sa.String(), nullable=False),
                    sa.Column('paper_count', sa.Integer(), nullable=False),
                    sa.Column('complete_fraction', sa.Float(), nullable=True),
                    sa.Column('complete_details', sa.Text(),
                              nullable=True),
                    sa.Column('created', UTCDateTime, nullable=True,
                              default=get_date),
                    sa.Column('updated', UTCDateTime, nullable=True,
                              onupdate=get_date),
                    sa.PrimaryKeyConstraint('summaryid'),
                    sa.UniqueConstraint('summaryid'))

    # ### end Alembic commands ###


def downgrade():
    match_status = postgresql.ENUM('Matched', 'Unmatched', 'NoIndex',
                                   name='match_status')
    match_type = postgresql.ENUM('canonical', 'deleted', 'alternate',
                                 'partial', 'mismatch', 'unmatched',
                                 'other', name='match_type')
    op.drop_table('master')
    op.drop_table('summary')

    match_status.drop(op.get_bind())
    match_type.drop(op.get_bind())

    # ### end Alembic commands ###
