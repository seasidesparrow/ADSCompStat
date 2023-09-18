import os
from adscompstat import utils
from adscompstat import tasks
from adscompstat.exceptions import *
from adscompstat.models import CompStatIdentDoi as identifier_doi
from adscompstat.models import CompStatAltIdents as alt_identifiers
from adscompstat.models import CompStatIssnBibstem as issn_bibstem
from adsputils import load_config, setup_logging
import argparse

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), './'))
conf = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                       level=conf.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=conf.get('LOG_STDOUT', False))

def get_arguments():
    parser = argparse.ArgumentParser(description='Command line options.')

    parser.add_argument('-p',
                        '--publisher-prefix',
                        dest='do_pub',
                        action='store',
                        default=None,
                        help='Parse only logs for one publisher DOI prefix')

    parser.add_argument('-l',
                        '--latest',
                        dest='do_latest',
                        action='store_true',
                        default=False,
                        help='Do only records from the most recent harvest')

    parser.add_argument('-c',
                        '--classic',
                        dest='do_load_classic',
                        action='store_true',
                        default=False,
                        help='Load bibstem/bibcode/doi/issn data from classic and journalsdb')
    parser.add_argument('-m',
                        '--completeness',
                        dest='do_completeness',
                        action='store_true',
                        default=False,
                        help='Calculate completeness summary for all harvested bibstems')
    parser.add_argument('-j',
                        '--json',
                        dest='do_json_export',
                        action='store_true',
                        default=False,
                        help='Export completeness summary to JSON file')

    args = parser.parse_args()
    return args

def get_logs(args):
    logfiles = utils.get_updateagent_logs(conf.get('HARVEST_LOG_DIR','/'))
    if logfiles:
        logfiles.sort()
        (dates, pubdois) = utils.parse_pub_and_date_from_logs(logfiles)
        if args.do_pub:
            if args.do_pub in pubdois:
                try:
                    newlogs = list()
                    for l in logfiles:
                        if args.do_pub in l:
                            newlogs.append(l)
                    logfiles = newlogs
                except Exception as err:
                    raise GetLogException('Problem selecting publisher (%s): %s' % (args.do_pub, err))
            else:
                raise GetLogException('No log files available for publisher %s' % args.do_pub)
        if args.do_latest:
            latestDate = dates[-1]
            try:
                newlogs = list()
                for l in logfiles:
                    if latestDate in l:
                        newlogs.append(l)
                logfiles = newlogs
            except Exception as err:
                raise GetLogException('Problem selecting most recent (%s): %s' % (latestDate, err))
        # elif args.do_since:
    return logfiles

def write_to_database(table_def, data):
    try:
        blocksize = conf.get("CLASSIC_DATA_BLOCKSIZE", 10000)
        if data and table_def:
            total_rows = len(data)
            while data:
                logger.debug("Writing to db: %s of %s rows remaining" %
                                (len(data), total_rows))
                insertblock = data[0:blocksize]
                data = data[blocksize:]
                tasks.task_write_block_to_db.delay(table_def, insertblock)
    except Exception as err:
        raise DBWriteException(err)


def load_classic_data():
    try:
        # Delete existing classic data store
        tasks.task_clear_classic_data()
    except Exception as err:
        raise DBClearException(err)
    else:

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

        # load bibstem-ISSN map
        infile = conf.get("JOURNALSDB_ISSN_BIBSTEM", None)
        records = utils.load_journalsdb_issn_bibstem_list(infile)
        if records:
            table_def = issn_bibstem
            write_to_database(table_def, records)
        else:
            raise LoadClassicDataException("No ISSN-bibstem data found.")

        # Alternate & deleted bibcode mappings
        infile_can = conf.get("CLASSIC_CANONICAL", None)
        infile_alt = conf.get("CLASSIC_ALTBIBS", None)
        infile_del = conf.get("CLASSIC_DELBIBS", None)
        infile_all = conf.get("CLASSIC_ALLBIBS", None)
        records = utils.merge_bibcode_lists(infile_can, infile_alt,
                                            infile_del, infile_all)
        if records:
            table_def = alt_identifiers
            write_to_database(table_def, records)
        else:
            raise LoadClassicDataException("No data from canonical/alt/deleted bibcode maps")



def main():
    try:
        args = get_arguments()

        if args.do_load_classic:
            #try:
            load_classic_data()
            #except Exception as err:
            #    logger.error("Failed to load classic data: %s" % err)

        elif args.do_completeness:
            tasks.task_do_all_completeness()
        elif args.do_json_export:
            tasks.task_export_completeness_to_json()
        else:
            logfiles = get_logs(args)
            if not logfiles:
                # logger.warn("No logfiles, nothing to do. Stopping.")
                print("No logfiles, nothing to do. Stopping.")
            else:
                for lf in logfiles:
                    tasks.task_process_logfile(lf)
    except Exception as err:
        # logger.warn("Completeness processing failed: %s" % err)
        print("Completeness processing failed: %s" % err)

if __name__ == '__main__':
    main()
