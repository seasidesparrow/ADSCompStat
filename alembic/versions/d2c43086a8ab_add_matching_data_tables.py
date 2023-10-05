"""Add matching data tables
Revision ID: d2c43086a8ab
Revises: 451c42f1578d
Create Date: 2023-09-21 12:57:00.000000
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
        "master",
        sa.Column("masterid", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("harvest_filepath", sa.String(), nullable=False),
        sa.Column("master_doi", sa.String(), nullable=False),
        sa.Column("issns", sa.Text(), nullable=True),
        sa.Column("db_origin", sa.String(), nullable=False),
        sa.Column("master_bibdata", sa.Text(), nullable=False),
        sa.Column("classic_match", sa.Text(), nullable=True),
        sa.Column(
            "status",
            postgresql.ENUM("Matched", "Unmatched", "NoIndex", "Failed", name="match_status"),
            nullable=False,
        ),
        sa.Column(
            "matchtype",
            postgresql.ENUM(
                "canonical",
                "deleted",
                "alternate",
                "partial",
                "mismatch",
                "unmatched",
                "other",
                "failed",
                name="match_type",
            ),
            nullable=False,
        ),
        sa.Column("bibcode_meta", sa.String(), nullable=True),
        sa.Column("bibcode_classic", sa.String(), nullable=True),
        sa.Column("notes", sa.String(), nullable=True),
        sa.Column("created", UTCDateTime, nullable=True, default=get_date),
        sa.Column("updated", UTCDateTime, nullable=True, onupdate=get_date),
        sa.PrimaryKeyConstraint("masterid"),
        sa.UniqueConstraint("master_doi"),
        sa.UniqueConstraint("masterid"),
    )

    # summary record for each bibstem, per volume
    op.create_table(
        "summary",
        sa.Column("summaryid", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("bibstem", sa.String(), nullable=False),
        sa.Column("volume", sa.String(), nullable=False),
        sa.Column("paper_count", sa.Integer(), nullable=False),
        sa.Column("complete_fraction", sa.Float(), nullable=True),
        sa.Column("complete_details", sa.Text(), nullable=True),
        sa.Column("created", UTCDateTime, nullable=True, default=get_date),
        sa.Column("updated", UTCDateTime, nullable=True, onupdate=get_date),
        sa.PrimaryKeyConstraint("summaryid"),
        sa.UniqueConstraint("summaryid"),
    )

    # storage for classic bibcode - doi
    op.create_table(
        "identifier_doi",
        sa.Column("identifier", sa.String(), index=True, primary_key=True, nullable=False),
        sa.Column("doi", sa.String(), index=True, primary_key=True, nullable=False),
        sa.PrimaryKeyConstraint("identifier", "doi"),
        sa.UniqueConstraint("doi"),
    )

    # storage for issn - classic bibstem
    op.create_table(
        "issn_bibstem",
        sa.Column("bibstem", sa.String(), index=True, primary_key=True, nullable=False),
        sa.Column("issn", sa.String(), index=True, primary_key=True, nullable=False),
        sa.Column("issn_type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("bibstem", "issn"),
    )

    # storage for canonical and noncanonical bibcodes
    op.create_table(
        "alt_identifiers",
        sa.Column("identifier", sa.String(), index=True, primary_key=True, nullable=False),
        sa.Column(
            "canonical_id",
            sa.String(),
            index=True,
            primary_key=True,
            nullable=False,
            server_default="",
        ),
        sa.Column("idtype", sa.String(), nullable=False, server_default=""),
        sa.PrimaryKeyConstraint("identifier", "canonical_id"),
    )

    # ### end Alembic upgrade commands ###


def downgrade():
    op.drop_table("alt_identifiers")
    op.drop_table("issn_bibstem")
    op.drop_table("identifier_doi")

    match_status = postgresql.ENUM(
        "Matched", "Unmatched", "NoIndex", "Failed", name="match_status"
    )
    match_type = postgresql.ENUM(
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
    op.drop_table("master")
    op.drop_table("summary")

    match_status.drop(op.get_bind())
    match_type.drop(op.get_bind())

    # ### end Alembic downgrade commands ###
