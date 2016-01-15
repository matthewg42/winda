#!/usr/bin/env python

import logging
import os
import sys
import wind.database
import wind.inputfile
from wind.database import Database

global args
global log

log = logging

def init_log():
    global log
    global args
    handler = logging.StreamHandler()
    fmt = ""
    if args.log_timestamps:
        fmt += '%%(asctime)s %s[%%(process)d] ' % os.path.basename(sys.argv[0])
    fmt += '%(levelname)s: %(message)s'
    handler.setFormatter(logging.Formatter(fmt))
    log = logging.getLogger('analyse_wind_data')
    log.setLevel(args.logging_level)
    log.addHandler(handler)
    wind.database.log = log
    wind.inputfile.log = log

def database_reset(args):
    log.warning('TODO: database_reset')

def database_info(args):
    d = Database(args.database_path)
    info = d.info()
    for k in ['Database file', 'Size', 'Number of files added', 'Number of records']:
        print('%-30s%s' % (k + ':', info[k]))
    
def add_files(args):
    log.warning('TODO: add_files')

def list_files(args):
    log.warning('TODO: list_files')

def remove_data(args):
    log.warning('TODO: remove_data')

def export_speeds(args):
    log.warning('TODO: export_speeds')

def export_data(args):
    log.warning('TODO: export_data')


#############
def add_data_filters(parser):
    parser.add_argument('--files', dest='file_filter', type=str, default=None,
                        help='Select data that came from a specific file, or a file matching this glob pattern')
    parser.add_argument('--date', dest='date_filter', type=str, default=None,
                        help='Select data only from a specific date in YYMMDD format')
    parser.add_argument('--from', dest='from', type=str, default=None,
                        help='Select data only after a specific date/time in YYMMDD[HHMMSS] format')
    parser.add_argument('--to', dest='to', type=str, default=None,
                        help='Select data only up to a specific date/time in YYMMDD[HHMMSS] format')

def add_files_option(parser):
    parser.add_argument('files', metavar='filename', type=str, nargs='+',
                   help='file names or glob pattern')

if __name__ == '__main__':
    import argparse
    global args

    parser = argparse.ArgumentParser(description='Re-Innovation Wind Data Analysis Tool')
    parser.add_argument('--database', dest='database_path', type=str, default='winda.db',
        help='specify the database file path')
    parser.add_argument('--debug', dest='logging_level', action='store_const',
        const=logging.DEBUG, default=logging.WARN,
        help='write debugging output in the log')
    parser.add_argument('--info', dest='logging_level', action='store_const',
        const=logging.INFO, help='write informational output in the log')
    parser.add_argument('--log-ts', dest='log_timestamps',
        action='store_const', const=True, default=False,
        help='write informational output in the log')
    parser.add_argument('--yes', dest='assume_yes', action='store_const', const=True, 
        default=False, help='Assume the answer to any confirmation prompt is YES')
    subparsers = parser.add_subparsers()

    # Reset command
    parser_reset = subparsers.add_parser('reset', help='Reset the database (delete everything!)')
    parser_reset.set_defaults(func=database_reset)

    # Info command
    parser_info = subparsers.add_parser('info', help='Print information about the database file and exit')
    parser_info.set_defaults(func=database_info)

    # Add command
    parser_add = subparsers.add_parser('add', help='Add data from CSV files into the database')
    parser_add.add_argument('files', metavar='filename', type=str, nargs='+',
                   help='file name or glob pattern to add to database')
    parser_add.set_defaults(func=add_files)

    # Files command
    parser_files = subparsers.add_parser('files', help='List files which have been added to the database')
    add_files_option(parser_files)
    parser_files.set_defaults(func=list_files)

    # Remove command
    parser_remove = subparsers.add_parser('remove', help='Remove data from the database')
    add_data_filters(parser_remove)
    parser_remove.set_defaults(func=remove_data)

    # Speeds command
    parser_speeds = subparsers.add_parser('speeds', help='Remove data from the database')
    parser_speeds.add_argument('--direction-split', dest='split', action='store_const', const=True, 
        default=False, help='Add a wind direction column to the output and perform the analysis for each wind direction found in the selected data')
    add_data_filters(parser_speeds)
    parser_speeds.set_defaults(func=export_speeds)

    # Export command
    parser_export = subparsers.add_parser('export', help='Remove data from the database')
    add_data_filters(parser_export)
    parser_export.add_argument('file', help='Specify the export file path')
    parser_export.set_defaults(func=export_data)

    # create the parser for the "b" command
    # parser_b = subparsers.add_parser('b', help='b help')
    # parser_b.add_argument('--baz', choices='XYZ', help='baz help')
    args = parser.parse_args()
    init_log()
    if not hasattr(args, 'func'):
        log.error('No command specified, try using --help')
    else:
        args.func(args)
    log.debug('END')

