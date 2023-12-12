import argparse
import os

from adsputils import load_config, setup_logging

from adscompstat import tasks, utils
from adscompstat.exceptions import (
    DBClearException,
    DBWriteException,
    GetLogException,
    LoadClassicDataException,
)
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatIssnBibstem as issn_bibstem

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "./"))
conf = load_config(proj_home=proj_home)
logger = setup_logging(
    "run.py",
    proj_home=proj_home,
    level=conf.get("LOGGING_LEVEL", "INFO"),
    attach_stdout=conf.get("LOG_STDOUT", False),
)


def get_arguments():
    parser = argparse.ArgumentParser(description="Command line options.")

    parser.add_argument(
        "-p",
        "--publisher-prefix",
        dest="do_pub",
        action="store",
        default=None,
        help="Parse only logs for one publisher DOI prefix",
    )

    parser.add_argument(
        "-l",
        "--latest",
        dest="do_latest",
        action="store_true",
        default=False,
        help="Do only records from the most recent harvest",
    )

    parser.add_argument(
        "-c",
        "--classic",
        dest="do_load_classic",
        action="store_true",
        default=False,
        help="Load bibstem/bibcode/doi/issn data from classic flat files",
    )
    parser.add_argument(
        "-m",
        "--completeness",
        dest="do_completeness",
        action="store_true",
        default=False,
        help="Calculate completeness summary for all harvested bibstems",
    )
    parser.add_argument(
        "-j",
        "--json",
        dest="do_json_export",
        action="store_true",
        default=False,
        help="Export completeness summary to JSON file",
    )
    parser.add_argument(
        "-r",
        "--retry",
        dest="do_retry",
        action="store_true",
        default=False,
        help="Retry all mismatched and unmatched records",
    )

    args = parser.parse_args()
    return args


def get_logs(args):
    logfiles = utils.get_updateagent_logs(conf.get("HARVEST_LOG_DIR", "/"))
    if logfiles:
        logfiles.sort()
        (dates, pubdois) = utils.parse_pub_and_date_from_logs(logfiles)
        if args.do_pub:
            if args.do_pub in pubdois:
                newlogs = list()
                try:
                    for logfile in logfiles:
                        if args.do_pub in logfile:
                            newlogs.append(logfile)
                except Exception as err:
                    raise GetLogException(
                        "Problem selecting publisher (%s): %s" % (args.do_pub, err)
                    )
                logfiles = newlogs
            else:
                raise GetLogException("No log files available for publisher %s" % args.do_pub)
        if args.do_latest:
            latestDate = dates[-1]
            newlogs = list()
            try:
                for logfile in logfiles:
                    if latestDate in logfile:
                        newlogs.append(logfile)
            except Exception as err:
                raise GetLogException("Problem selecting most recent (%s): %s" % (latestDate, err))
            logfiles = newlogs
    return logfiles


def write_to_database(table_def, data):
    try:
        blocksize = conf.get("CLASSIC_DATA_BLOCKSIZE", 10000)
        total_rows = len(data)
        if data and table_def:
            i = 0
            while i < total_rows:
                logger.debug(
                    "Writing to db: %s of %s rows remaining" % (len(data) - i, total_rows)
                )
                insertblock = data[i : (i + blocksize)]
                tasks.task_write_block_to_db(table_def, insertblock)
                i += blocksize
    except Exception as err:
        raise DBWriteException(err)


def load_classic_data():
    try:
        # Delete existing classic data store
        tasks.task_clear_classic_data()
    except Exception as err:
        raise DBClearException(err)
    else:
        # load bibstem-ISSN map
        infile = conf.get("JOURNALSDB_ISSN_BIBSTEM", None)
        records = utils.load_journalsdb_issn_bibstem_list(infile)
        if records:
            table_def = issn_bibstem
            write_to_database(table_def, records)
        else:
            raise LoadClassicDataException("No ISSN-bibstem data found.")

        # load bibcode-DOI map
        infile = conf.get("CLASSIC_DOI_FILE", None)
        if infile:
            records = utils.load_classic_doi_bib_map(infile)
        else:
            logger.warning("No CLASSIC_DOI_FILE name given.")
        if records:
            table_def = identifier_doi
            write_to_database(table_def, records)
        else:
            raise LoadClassicDataException("No DOI-bibcode data found.")

        # load alternate and deleted bibcode mappings
        infile_can = conf.get("CLASSIC_CANONICAL", None)
        infile_alt = conf.get("CLASSIC_ALTBIBS", None)
        infile_del = conf.get("CLASSIC_DELBIBS", None)
        infile_all = conf.get("CLASSIC_ALLBIBS", None)
        records = utils.merge_bibcode_lists(infile_can, infile_alt, infile_del, infile_all)
        if records:
            table_def = alt_identifiers
            write_to_database(table_def, records)
        else:
            raise LoadClassicDataException("No data from canonical/alt/deleted bibcode maps")


def main():
    try:
        args = get_arguments()

        if args.do_load_classic:
            try:
                load_classic_data()
            except Exception as err:
                logger.error("Failed to load classic data: %s" % err)

        elif args.do_completeness:
            tasks.task_do_all_completeness()
        elif args.do_json_export:
            tasks.task_export_completeness_to_json()
        elif args.do_retry:
            for result_type in ["mismatch", "unmatched", "failed"]:
                tasks.task_retry_records.delay(result_type)
        else:
            logfiles = get_logs(args)
            if not logfiles:
                logger.warn("No logfiles found! Nothing to do -- stopping.")
            else:
                for logfile in logfiles:
                    tasks.task_process_logfile.delay(logfile)
    except Exception as err:
        logger.error("Process failed: %s" % err)


if __name__ == "__main__":
    main()
