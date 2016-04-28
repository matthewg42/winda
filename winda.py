#!/usr/bin/env python

import logging
import os
import sys
import wind.database
import wind.inputfile
import wind.filter
import fileinput
import fnmatch
import dateutil.parser
from wind.database import Database, result_as_dict_array, result_headers
from wind.filter import Filter

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
    wind.filter.log = log

def database_reset(args):
    d = Database(args.database_path)
    if confirmation():
        d.reset()
    else:
        log.warning("Database reset ABORTED because confirmation not given")

def database_info(args):
    d = Database(args.database_path)
    info = d.info()
    for k in ['Database file', 'Size', 'Number of files added', 'Number of records']:
        print('%-30s%s' % (k + ':', info[k]))
    
def add_files(args):
    d = Database(args.database_path)
    d.add(args.files)

def show_files(args):
    d = Database(args.database_path)

    def want(path, patterns):
        if len(patterns) == 0:
            return True
        dirname, basename = os.path.split(path)
        for pat in patterns:
            if fnmatch.fnmatch(basename, pat):
                return True
        return False

    print(args.files)
    print('%3s %-50s %19s %9s %9s' % (
            'ID', 'PATH', 'IMPORTED', 'RECORDS', 'ERRORS'))
    print(' '.join(['-' * l for l in [3, 50, 19, 9, 9]]))
    for r in d.list_input_files():
        if want(r['path'], args.files):
            print('%3d %-50s %19s %9d %9d' % (
                    r['id'], r['path'], r['import_date'][:19], r['records'], r['errors']))
    
def remove_data(args):
    d = Database(args.database_path)
    c = d._conn.cursor()
    filt = generate_filter(args, c)

    # populate the temp tables tmp_event_rids, tmp_raw_data_rids
    filt.select_events()
    filt.select_raw_data()

    c.execute("""SELECT rid FROM tmp_event_rids""")
    event_rowids = c.fetchall()
    c.execute("""SELECT rid FROM tmp_raw_data_rids""")
    raw_data_rowids = c.fetchall()
    if confirmation('Remove %d events and %d raw_date records (y/N)? ' % (
                    len(event_rowids),
                    len(raw_data_rowids))):
        c.execute("""
                  DELETE FROM       event
                  WHERE             EXISTS (
                      SELECT            1
                      FROM              tmp_event_rids t
                      WHERE             t.rid = event.rowid
                  )
                  """)
        c.execute("""
                  DELETE FROM       raw_data
                  WHERE             EXISTS (
                      SELECT            1
                      FROM              tmp_raw_data_rids t
                      WHERE             t.rid = raw_data.rowid
                  )
                  """)
        c.execute("""
                  SELECT       id, path
                  FROM         input_file
                  WHERE        NOT EXISTS (
                      SELECT       1
                      FROM         event e
                      WHERE        e.file_id = input_file.id
                  )
                  AND          NOT EXISTS (
                      SELECT       1
                      FROM         raw_data r
                      WHERE        r.file_id = input_file.id
                  )""")
        for r in c.fetchall():
            print('Removing input file %s as it no longer has events / raw_data' % r[1])
        c.execute("""
                  DELETE FROM  input_file
                  WHERE        NOT EXISTS (
                      SELECT       1
                      FROM         event e
                      WHERE        e.file_id = input_file.id
                  )
                  AND          NOT EXISTS (
                      SELECT       1
                      FROM         raw_data r
                      WHERE        r.file_id = input_file.id
                  )""")
        d.commit()

def export_speeds(args):
    if args.increment <= 0.0:
        raise Exception('--increment must be greater than 0.0')
    d = Database(args.database_path)
    c = d._conn.cursor()
    # Create a temp table with the windspeed ranges to analyse
    c.execute("""DROP TABLE IF EXISTS windspeed_range""")
    c.execute("""CREATE TEMP TABLE windspeed_range (a FLOAT UNIQUE, b FLOAT UNIQUE)""")
    rng = [float(i) for i in args.range.split('-')]
    a = rng[0] 
    while a < rng[1]:
        b = a+args.increment
        c.execute("""
                  INSERT OR IGNORE INTO windspeed_range 
                  VALUES (?, ?)
                  """, (a, b))
        log.debug('adding range: %f to %f' % (a, b))
        a = b
    
    filt = generate_filter(args, c)
    filt.select_events()

    result_csv = []
    if args.split:
        groups = 'e.wind_direction, r.a, r.b'
        result_csv.append('direction,windspeed_range_begin,windspeed_range_end,probability')
    else:
        groups = 'r.a, r.b'
        result_csv.append('windspeed_range_begin,windspeed_range_end,probability')

    total = filt.count_selected_events()
    wind_field = 'windspeed_ms_%d' % args.anemometer_no
    c.execute("""
              SELECT          %s, COUNT(1)
              FROM            event e,
                              windspeed_range r
              WHERE           %s >= r.a
              AND             %s < r.b
              AND EXISTS (
                  SELECT        1
                  FROM          tmp_event_rids t
                  WHERE         t.rid = e.rowid
              )
              GROUP BY        %s
              """ % (groups, wind_field, wind_field, groups))
    log.debug('total selected events: %d' % total)
    for r in c.fetchall():
        if args.split:
            result_csv.append('%s,%.2f,%.2f,%.4f' % (r[0], r[1], r[2], float(r[3])/float(total)))
        else:
            result_csv.append('%.2f,%.2f,%.4f' % (r[0], r[1], float(r[2])/float(total)))
    print('\n'.join(result_csv))

def export_average(args):
    d = Database(args.database_path)
    c = d._conn.cursor()
    filt = generate_filter(args, c)
    filt.select_events()
    wind_field = 'windspeed_ms_%d' % args.anemometer_no
    if args.split:
        c.execute("""
                  SELECT          wind_direction, avg(%s), count(1)
                  FROM            event e,
                                  tmp_event_rids t
                  WHERE           t.rid = e.rowid
                  GROUP BY        wind_direction
                  """ % wind_field)
    else:
        c.execute("""
                  SELECT          avg(%s), count(1)
                  FROM            event e,
                                  tmp_event_rids t
                  WHERE           t.rid = e.rowid
                  """ % wind_field)
    print(','.join(result_headers(c)))
    for r in c.fetchall():
        print(','.join([str(i) for i in list(r)]))

def export_data(args):
    d = Database(args.database_path)
    c = d._conn.cursor()
    filt = generate_filter(args, c)
    filt.select_events()
    result_csv = []
    c.execute("""
              SELECT          ref, event_start, event_end, windspeed_ms_1, windspeed_ms_2, wind_direction, irradiance_wm2
              FROM            event e
              WHERE           EXISTS (
                  SELECT        1
                  FROM          tmp_event_rids t
                  WHERE         t.rid = e.rowid
              )
              """)
    print(','.join(result_headers(c)))
    for r in c.fetchall():
        print(','.join([str(i) for i in list(r)]))

def calibrate(args):
    log.debug('calibrate(%s)' % args.ref)
    d = Database(args.database_path)
    c = d._conn.cursor()
    c.execute("""SELECT 1 FROM calibration WHERE ref = ?""", (args.ref,))
    if len(c.fetchall()) > 0:
        log.debug('calibrate() updating existing record for ref %s' % args.ref)
        c.execute("""
                  UPDATE        calibration
                  SET           anemometer_1_factor = ?,
                                anemometer_2_factor = ?,
                                max_windspeed_ms = ?,
                                irradiance_factor = ?,
                                max_irradiance = ?
                  WHERE         ref = ?
                  """, (args.anemometer_1_factor,
                        args.anemometer_2_factor,
                        args.max_windspeed_ms,
                        args.irradiance_factor,
                        args.max_irradiance,
                        args.ref))
    else:
        log.debug('calibrate() inserting new record for ref %s' % args.ref)
        c.execute("""
                  INSERT INTO   calibration (
                                ref, 
                                anemometer_1_factor, 
                                anemometer_2_factor, 
                                max_windspeed_ms, 
                                irradiance_factor, 
                                max_irradiance
                  )
                  VALUES        ( ?, ?, ?, ?, ?, ? )
                  """, (args.ref, 
                        args.anemometer_1_factor,
                        args.anemometer_2_factor,
                        args.max_windspeed_ms,
                        args.irradiance_factor,
                        args.max_irradiance))
    d.commit()

def show_calibration(args):
    log.debug('show_calibration(%s)' % args.ref)
    d = Database(args.database_path)
    c = d._conn.cursor()
    c.execute("""
              SELECT     ref, anemometer_1_factor, anemometer_2_factor, 
                         max_windspeed_ms, irradiance_factor, max_irradiance 
              FROM       calibration 
              WHERE      ref LIKE ?
              ORDER BY   ref
              """, (args.ref,))
    headers = result_headers(c)
    headerlen = [len(h) for h in headers]
    print(' '.join(headers))
    print(' '.join(['-' * hl for hl in headerlen]))
    for r in c.fetchall():
        print('%-3s %19.3f %19.3f %16.3f %17.3f %14.3f' % tuple(r))

#############
def add_data_filters(parser):
    parser.add_argument('--files', dest='file_filter', type=str, default=None,
                        help='Select data that came from a specific file, or a file matching this glob pattern')
    parser.add_argument('--date', dest='date_filter', type=str, default=None,
                        help='Select data only from a specific date in YYMMDD format')
    parser.add_argument('--from', dest='from_filter', type=str, default=None,
                        help='Select data only after a specific date/time in YYMMDD[HHMMSS] format')
    parser.add_argument('--to', dest='to_filter', type=str, default=None,
                        help='Select data only up to a specific date/time in YYMMDD[HHMMSS] format')

def generate_filter(args, cursor):
    file_filter = args.file_filter

    date_filter = None
    # date_filter should be a datetime.date() object
    if args.date_filter is not None:
        date_filter = dateutil.parser.parse(args.date_filter).date()

    # from_filter and to_filter should be datetime.datetime() objects
    from_filter = None
    to_filter = None
    if args.from_filter is not None:
        from_filter = dateutil.parser.parse(args.from_filter)
    if args.to_filter is not None:
        to_filter = dateutil.parser.parse(args.to_filter)

    log.debug('file_filter = %s : %s' % (type(file_filter), file_filter))
    log.debug('date_filter = %s : %s' % (type(date_filter), date_filter))
    log.debug('from_filter = %s : %s' % (type(from_filter), from_filter))
    log.debug('to_filter = %s : %s' % (type(to_filter), to_filter))
    return Filter(cursor,
                  file_filter=file_filter, 
                  date_filter=date_filter,
                  from_filter=from_filter, 
                  to_filter=to_filter)

def add_files_option(parser):
    parser.add_argument('files', metavar='filename', type=str, nargs='*',
                   help='file names or glob pattern')

def confirmation(msg='Are you sure (y/N)? '):
    """Prompt the user for confirmation of some operation, return True if OK to proceed."""
    if args.assume_yes:
        return True
    while True:
        sys.stdout.write(msg)
        response = raw_input()
        if response.lower() in ['y', 'yes']:
            return True
        elif response.lower() in ['n', 'no', '']:
            return False
        else:
            print("Please choose y or n")

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

    # Remove command
    parser_remove = subparsers.add_parser('remove', help='Remove data from the database')
    add_data_filters(parser_remove)
    parser_remove.set_defaults(func=remove_data)

    # Speeds command
    parser_speeds = subparsers.add_parser('speeds', help='Output wind speed transform')
    parser_speeds.add_argument('--direction-split', dest='split', action='store_const', const=True, 
        default=False, help='Add a wind direction column to the output and perform the analysis for each wind direction found in the selected data')
    parser_speeds.add_argument('--range', dest='range', type=str, default='0-40', 
        help='Specify the range of windspeeds to analyse, e.g. 0-20.5')
    parser_speeds.add_argument('--increment', dest='increment', type=float, default=0.5, 
        help='Specify the incremement of windspeeds to analyse, e.g. 0.5')
    parser_speeds.add_argument('--2', dest='anemometer_no', action='store_const', const=2, default=1, 
        help='Use data from the second anemometer (the first is the default)')
    add_data_filters(parser_speeds)
    parser_speeds.set_defaults(func=export_speeds)

    # Average command
    parser_average = subparsers.add_parser('average', help='Output average (mean) wind speed')
    parser_average.add_argument('--direction-split', dest='split', action='store_const', const=True, 
        default=False, help='Add a wind direction column to the output and perform the analysis for each wind direction found in the selected data')
    parser_average.add_argument('--2', dest='anemometer_no', action='store_const', const=2, default=1, 
        help='Use data from the second anemometer (the first is the default)')
    add_data_filters(parser_average)
    parser_average.set_defaults(func=export_average)

    # Export command
    parser_export = subparsers.add_parser('export', help='Remove data from the database')
    add_data_filters(parser_export)
    parser_export.set_defaults(func=export_data)

    # Calibrate command
    parser_calibrate = subparsers.add_parser('calibrate', help='Calibrate a sensor/Ref')
    parser_calibrate.add_argument('ref', help='Value of ref, e.g. "BB"')
    parser_calibrate.add_argument('anemometer_1_factor', type=float, help='Value of anemometer_1_factor, e.g. "1.42"')
    parser_calibrate.add_argument('anemometer_2_factor', type=float, help='Value of anemometer_2_factor, e.g. "1.42"')
    parser_calibrate.add_argument('max_windspeed_ms', type=float, help='Value of max_windspeed_ms, e.g. "100"')
    parser_calibrate.add_argument('irradiance_factor', type=float, help='Value of irradiance_factor, e.g. "1.0"')
    parser_calibrate.add_argument('max_irradiance', type=float, help='Value of max_irradiance, e.g. "1500"')
    parser_calibrate.set_defaults(func=calibrate)

    # Show command
    parser_show = subparsers.add_parser('show', help='Show various things')
    show_subparsers = parser_show.add_subparsers()
    
    # show calibration command
    parser_show_calibration = show_subparsers.add_parser('calibration', help='Show current calibration(s)')
    parser_show_calibration.add_argument('ref', type=str, nargs='?', default='%', help='Filter by this ref')
    parser_show_calibration.set_defaults(func=show_calibration)

    # show files command
    parser_show_files = show_subparsers.add_parser('files', help='Show files in the database') 
    add_files_option(parser_show_files)
    parser_show_files.set_defaults(func=show_files)

    # Do it!
    args = parser.parse_args()
    init_log()
    if not hasattr(args, 'func'):
        log.error('No command specified, try using --help')
    else:
        args.func(args)
    log.debug('END')

