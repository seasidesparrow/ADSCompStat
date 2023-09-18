"""Add matching data tables
Revision ID: d2c43086a8ab
Revises: f0123456789a
Create Date: 2022-08-15 12:00:00.000000
"""
from alembic import op
from adsputils import UTCDateTime, get_date
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'd2c43086a8ab'
down_revision = 'f0123456789a'
branch_labels = None
depends_on = None


def upgrade():
    # master record for each doi
    op.create_table('identifier_doi',
                    sa.Column('identifier', sa.String(), index=True,
                              primary_key=True, nullable=False),
                    sa.Column('doi', sa.String(), index=True,
                              primary_key=True, nullable=False),
                    sa.PrimaryKeyConstraint('identifier', 'doi'),
                    sa.UniqueConstraint('doi')
                   )

    op.create_table('issn_bibstem',
                    sa.Column('bibstem', sa.String(), index=True,
                              primary_key=True, nullable=False),
                    sa.Column('issn', sa.String(), index=True,
                              primary_key=True, nullable=False),
                    sa.Column('issn_type', sa.String(), nullable=False),
                    sa.PrimaryKeyConstraint('bibstem', 'issn')
                   )

    op.create_table('alt_identifiers',
                    sa.Column('identifier', sa.String(), index=True,
                              primary_key=True, nullable=False),
                    sa.Column('canonical_id', sa.String(), index=True,
                              primary_key=True, nullable=False,
                              server_default=''),
                    sa.Column('idtype', sa.String(), nullable=False,
                              server_default=''),
                    sa.PrimaryKeyConstraint('identifier', 'canonical_id')
                   )

    # ### end Alembic commands ###


def downgrade():

    op.drop_table('alt_identifiers')
    op.drop_table('issn_bibstem')
    op.drop_table('identifier_doi')

    # ### end Alembic commands ###

