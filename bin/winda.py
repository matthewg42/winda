#!/usr/bin/env python3

import logging
import os
import sys

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

def database_reset(args):
    log.warning('TODO: database_reset')

def database_info(args):
    log.warning('TODO: database_info')

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

    # create the parser for the "b" command
    # parser_b = subparsers.add_parser('b', help='b help')
    # parser_b.add_argument('--baz', choices='XYZ', help='baz help')
    args = parser.parse_args()
    init_log()
    args.func(args)
    log.debug('END')

