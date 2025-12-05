"""Add matching data tables
Revision ID: 0b91d0dcaadf
Revises: d2c43086a8ab
Create Date: 2025-12-05 12:57:00.000000
"""
import sqlalchemy as sa
from adsputils import UTCDateTime, get_date
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "d2c43086a8ab"
down_revision = "451c42f1578d"
branch_labels = None
depends_on = None


def upgrade():
    # master record for each doi
    op.create_table(
        "master_new",
        sa.Column("masterid", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("harvest_filepath", sa.String(), nullable=False),
        sa.Column("master_doi", sa.String(), nullable=False),
        sa.Column("issns", postgresql.JSONB(), nullable=True),
        sa.Column("db_origin", sa.String(), nullable=False),
        sa.Column("master_bibdata", postgresql.JSONB(), nullable=False),
        sa.Column("doi_found", sa.Boolean(), nullable=True),
        sa.Column("record_matched", sa.Boolean(), nullable=True),
        sa.Column("master_record_id", sa.String(), nullable=True),
        sa.Column("notes", sa.String(), nullable=True),
        sa.Column("created", UTCDateTime, nullable=True, default=get_date),
        sa.Column("updated", UTCDateTime, nullable=True, onupdate=get_date),
        sa.PrimaryKeyConstraint("masterid"),
        sa.UniqueConstraint("master_doi"),
        sa.UniqueConstraint("masterid"),
        sa.UniqueConstraint("master_record_id"),
    )

    # ### end Alembic upgrade commands ###


def downgrade():
    op.drop_table("master_new")

    # ### end Alembic downgrade commands ###
