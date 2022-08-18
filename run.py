from adscompstat import utils
from adscompstat import tasks
from adscompstat.exceptions import GetLogException
from config import *
import argparse
import json

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

    args = parser.parse_args()
    return args

def get_logs(args):
    logfiles = utils.get_updateagent_logs(HARVEST_LOG_DIR)
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
        logfiles = get_logs(args)
    except Exception as err:
        print('Completeness processing failed: %s' % err)
    else:
        if not logfiles:
            print('No logfiles, nothing to do. Stopping.')
        else:
            with open(OUTPUT_MAP_FILENAME, 'w') as fout:
                for lf in logfiles:
                    output = tasks.task_process_logfile(lf)
                    for rec in output:
                        fout.write("%s\n" % rec)


if __name__ == '__main__':
    main()
