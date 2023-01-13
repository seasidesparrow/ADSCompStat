from adscompstat import utils
from adscompstat import tasks
from adscompstat.exceptions import GetLogException
from adsputils import load_config
from config import *
import argparse

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
conf = load_config(proj_home=proj_home)

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
    logfiles = utils.get_updateagent_logs(conf.get('HARVEST_LOG_DIR','/data/Crossref/UpdateAgent/'))
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


def main():
    try:
        args = get_arguments()
        if args.do_load_classic:
            tasks.task_load_classic_data()
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
