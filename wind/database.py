"""Provide database functionality using sqlite3."""

import sqlite3
import os
import logging
import glob
from datetime import datetime
from wind.inputfile import InputFile

log = logging

class Database:
    def __init__(self, path):
        """ 
        Open an existing database or, if no database exists at 
        the specified path, create a new one with the appropriate 
        schema.
        """
        log.debug('Constructing Database object for path: %s' % path)
        self._path = path
        self._conn = sqlite3.connect(self._path)
        # Make it so we can do a single transaction with multiple executes...
        self._conn.isolation_level = None
        if not self.schema_exists():
            self.create_schema()

    def __exit__(self, exc_type, exc_value, traceback):
        # Commit changes to the database
        log.debug('Database.__exit__: committing changes to %s' % self._path)
        self.commit()
        log.debug('Database.__exit__: commit complete')

    def commit(self, cur=None):
        if cur is not None:
            try:
                cur.execute("commit")
            except Exception as e:
                log.debug('Database.commit(cur=%s) cur.commit() exception [ignoring]: %s' % (cur, e))
                pass
        try:
            self._conn.commit()
        except Exception as e:
            log.debug('Database._conn.commit() exception [ignoring]: %s' % e)
            pass

    def schema_exists(self):
        """
        Test if a winda schema exists in the database at the path for this object.

        Return True if the database looks like a valid winda database, else
        return False.
        """
        c = self._conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE name LIKE 'winda_schema_v_%';")
        if len(c.fetchall()) == 0:
            return False
        else:
            return True

    def create_schema(self):
        """Create a new, empty database at the specified path."""
        log.debug('create_schema -> %s' % self._path)
        c = self._conn.cursor()
        log.debug('creating table calibration...')
        c.execute("""
                  CREATE TABLE calibration (
                      ref VARCHAR(12) PRIMARY KEY,
                      anemometer_1_factor FLOAT,
                      anemometer_2_factor FLOAT,
                      max_windspeed_ms FLOAT,
                      irradiance_factor FLOAT,
                      max_irradiance FLOAT
                  )
                  """)

        log.debug('creating table field_mapping...')
        c.execute("""
                  CREATE TABLE field_mapping (
                      header VARCHAR(128) UNIQUE,
                      field VARCHAR(128) NOT NULL,
                      cast_type VARCHAR(12)
                  )
                  """)

        log.debug('creating table input_file...')
        c.execute("""
                  CREATE TABLE input_file (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      path VARCHAR(255),
                      import_date DATETIME,
                      records INTEGER,
                      errors INTEGER
                  )
                  """)

        log.debug('creating table raw_data...')
        c.execute("""
                  CREATE TABLE raw_data (
                      file_id INTEGER,
                      ref VARCHAR(12),
                      dt CHAR(10),
                      tm CHAR(8), 
                      wind_1 INTEGER,
                      wind_2 INTEGER,
                      direction VARCHAR(2),
                      irradiance FLOAT,
                      batt_v FLOAT,
                      processed BOOLEAN,
                      ts DATETIME,
                      FOREIGN KEY(file_id) REFERENCES input_file(id)
                  )
                  """)
        log.debug('creating index on raw_data.ts...')
        c.execute("""CREATE INDEX raw_data__ts ON raw_data(ts)""")

        log.debug('creating table event...')
        c.execute("""
                  CREATE TABLE event (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      ref VARCHAR(12),
                      file_id INTEGER,
                      event_start DATETIME,
                      event_end DATETIME,
                      event_duration FLOAT,
                      anemometer_hz_1 FLOAT,
                      anemometer_hz_2 FLOAT,
                      irradiance_v FLOAT,
                      windspeed_ms_1 FLOAT,
                      windspeed_ms_2 FLOAT,
                      wind_direction VARCHAR(2),
                      irradiance_wm2 FLOAT,
                      FOREIGN KEY(ref) REFERENCES calibrations(ref),
                      FOREIGN KEY(file_id) REFERENCES input_file(id)
                  )
                  """)

        log.debug('populating calibration...')
        c.execute("""
                  INSERT INTO calibration (
                      ref,  
                      anemometer_1_factor, 
                      anemometer_2_factor, 
                      max_windspeed_ms, 
                      irradiance_factor, 
                      max_irradiance
                  )
                  VALUES (
                      'BB', 
                      1.42,               
                      1.42,                
                      100,              
                      1.0,               
                      1500
                  )
                  """)

        log.debug('populating field_mapping...')
        c.execute("""
                  INSERT INTO field_mapping
                      SELECT 'wind' AS 'header', 'wind_1' AS 'field', 'int' as cast_type
                      UNION ALL SELECT 'wind speed', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_speed', 'wind_1', 'int'
                      UNION ALL SELECT 'windspeed', 'wind_1', 'int'
                      UNION ALL SELECT 'wind ticks', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_ticks', 'wind_1', 'int'
                      UNION ALL SELECT 'windticks', 'wind_1', 'int'
                      UNION ALL SELECT 'wind pulses', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_pulses', 'wind_1', 'int'
                      UNION ALL SELECT 'windpulses', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer hz', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer_hz', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometerhz', 'wind_1', 'int'
                      UNION ALL SELECT 'ticks', 'wind_1', 'int'
                      UNION ALL SELECT 'pulses', 'wind_1', 'int'
                      UNION ALL SELECT 'wind 1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind speed 1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_speed_1', 'wind_1', 'int'
                      UNION ALL SELECT 'windspeed1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind ticks 1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_ticks_1', 'wind_1', 'int'
                      UNION ALL SELECT 'windticks1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind pulses 1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind_pulses_1', 'wind_1', 'int'
                      UNION ALL SELECT 'windpulses1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer 1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer_1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer hz 1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometer_hz_1', 'wind_1', 'int'
                      UNION ALL SELECT 'anemometerhz1', 'wind_1', 'int'
                      UNION ALL SELECT 'ticks 1', 'wind_1', 'int'
                      UNION ALL SELECT 'ticks_1', 'wind_1', 'int'
                      UNION ALL SELECT 'ticks1', 'wind_1', 'int'
                      UNION ALL SELECT 'pulses 1', 'wind_1', 'int'
                      UNION ALL SELECT 'pulses_1', 'wind_1', 'int'
                      UNION ALL SELECT 'pulses1', 'wind_1', 'int'
                      UNION ALL SELECT 'wind 2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind_2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind speed 2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind_speed_2', 'wind_2', 'int'
                      UNION ALL SELECT 'windspeed2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind ticks 2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind_ticks_2', 'wind_2', 'int'
                      UNION ALL SELECT 'windticks2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind pulses 2', 'wind_2', 'int'
                      UNION ALL SELECT 'wind_pulses_2', 'wind_2', 'int'
                      UNION ALL SELECT 'windpulses2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometer 2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometer_2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometer2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometer hz 2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometer_hz_2', 'wind_2', 'int'
                      UNION ALL SELECT 'anemometerhz2', 'wind_2', 'int'
                      UNION ALL SELECT 'irradiance', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance v', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance_v', 'irradiance', 'float'
                      UNION ALL SELECT 'irr', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance wm-2', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance_wm-2', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiancewm-2', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiancewm2', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance wm2', 'irradiance', 'float'
                      UNION ALL SELECT 'irradiance_wm2', 'irradiance', 'float'
                      UNION ALL SELECT 'batt', 'batt_v', 'float'
                      UNION ALL SELECT 'batt_v', 'batt_v', 'float'
                      UNION ALL SELECT 'batt v', 'batt_v', 'float'
                      UNION ALL SELECT 'battery', 'batt_v', 'float'
                      UNION ALL SELECT 'battery v', 'batt_v', 'float'
                      UNION ALL SELECT 'battery_v', 'batt_v', 'float'
                      UNION ALL SELECT 'batt_volts', 'batt_v', 'float'
                      UNION ALL SELECT 'batt volts', 'batt_v', 'float'
                      UNION ALL SELECT 'battery volts', 'batt_v', 'float'
                      UNION ALL SELECT 'battery_volts', 'batt_v', 'float'
                      UNION ALL SELECT 'ref', 'ref', NULL
                      UNION ALL SELECT 'reference', 'ref', NULL
                      UNION ALL SELECT 'dt', 'dt', NULL
                      UNION ALL SELECT 'date', 'dt', NULL
                      UNION ALL SELECT 'tm', 'tm', NULL
                      UNION ALL SELECT 'time', 'tm', NULL
                      UNION ALL SELECT 'direction', 'direction', NULL
                      UNION ALL SELECT 'dir', 'direction', NULL
                  """)
        
        log.debug('creating table winda_schema_v_1_00...')
        c.execute("""CREATE TABLE winda_schema_v_1_00 (id INT UNIQUE)""")

    def info(self):
        """Return a dict with some helpful information about the database."""
        d = dict()
        d['Database file'] = self._path
        d['Size'] = os.path.getsize(self._path)
        c = self._conn.cursor()
        c.execute("""SELECT DISTINCT id from input_file""")
        d['Number of files added'] = len(c.fetchall())
        c.execute("""SELECT 1 FROM event""")
        d['Number of records'] = len(c.fetchall())
        return d

    def add(self, patterns):
        """Add a list of glob patterns or files to the datbase."""
        for pattern in patterns:
            self.add_pattern(pattern)
        
    def add_pattern(self, pattern):
        """Add a single glob pattern of files to the datbase."""
        log.debug('Database.add_pattern(%s)' % pattern)
        for path in glob.glob(pattern):
            self.add_file(path)

    def add_file(self, path):
        """Add a single CSV file to the raw_data table."""
        log.debug('Database.add_file(%s)' % path)
        infile = InputFile(path, self)
        c = self._conn.cursor()
        c.execute("""INSERT INTO input_file (path, import_date, records, errors)
                     VALUES (?, ?, NULL, NULL)""", (path, datetime.now()))
        try:
            c.execute("""SELECT id FROM input_file WHERE path = ?""", (path,))
            result = c.fetchall()
            file_id = result[0][0]
        except Exception as e:
            log.error('Failed to get id for input file: %s / %s' % (type(e), e))
            raise

        c.execute('begin')
        record_count, error_count = 0, 0
        for record in infile:
            record_count += 1
            try:
                c.execute("""
                          INSERT OR IGNORE INTO raw_data (
                              file_id,
                              ref,
                              dt,
                              tm,
                              wind_1,
                              wind_2,
                              direction,
                              irradiance,
                              batt_v,
                              processed,
                              ts
                          )
                          VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ? )
                          """, (
                                file_id, 
                                record['ref'],
                                record['dt'],
                                record['tm'],
                                record['wind_1'],
                                record['wind_2'],
                                record['direction'],
                                record['irradiance'],
                                record['batt_v'],
                                datetime.strptime("%sT%s" % (record['dt'], record['tm']), "%d-%m-%YT%H:%M:%S")
                               ))
            except Exception as e:
                log.warning('Database.add_file, failed to add record: %s, exception: %s' % (record, e))
                error_count += 1
        # Update input_file record to reflect 
        c.execute("""UPDATE input_file SET records = ?, errors = ? WHERE path = ?""", (record_count, error_count, path))
        self.commit(c)
        self.process_file(file_id)

    def process_file(self, file_id):
        q = self._conn.cursor()
        calibration = self.get_calibration()
        q.execute("""
                  SELECT     ts, ref, wind_1, wind_2, 
                             direction, irradiance, batt_v, rowid
                  FROM       raw_data 
                  WHERE      file_id = ?
                  AND        processed = 0
                  ORDER BY ts ASC
                  """, (file_id,))
        prev_ts = None
        c = self._conn.cursor()
        c.execute('begin')
        for r in q.fetchall():
            try:
                if prev_ts is None:
                    prev_ts = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
                    continue
                ts = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
                delta_t = ts - prev_ts
                if delta_t.seconds == 0:
                    # no time since previous record - SKIP
                    log.warning('Database.process_file(%s) multiple records for time: %s, SKIPPING after first' % (
                                    file_id, ts))
                    continue
                elapsed = float(delta_t.seconds)
                wind_1_hz = r[2] / elapsed
                wind_2_hz = r[3] / elapsed
                windspeed_ms_1 = wind_1_hz * calibration[r[1]]['anemometer_1_factor']
                windspeed_ms_2 = wind_2_hz * calibration[r[1]]['anemometer_2_factor']
                irradiance_wm2 = r[5] * calibration[r[1]]['irradiance_factor']
                if windspeed_ms_1 > calibration[r[1]]['max_windspeed_ms']:
                    raise Exception('Spurious windspeed 1 (%.3f > %.3f)' % (windspeed_ms_1, calibration[r[1]]['max_windspeed_ms']))
                if windspeed_ms_2 > calibration[r[1]]['max_windspeed_ms']:
                    raise Exception('Spurious windspeed 2 (%.3f > %.3f)' % (windspeed_ms_2, calibration[r[1]]['max_windspeed_ms']))
                if irradiance_wm2 > calibration[r[1]]['max_irradiance']:
                    raise Exception('Spurious irradiance_wm2 (%.3f > %.3f)' % (irradiance_wm2, calibration[r[1]]['max_irradiance']))
                subs = (r[1], 
                        file_id, 
                        prev_ts, 
                        ts, 
                        elapsed, 
                        wind_1_hz,
                        wind_2_hz,
                        r[5],
                        windspeed_ms_1,
                        windspeed_ms_2,
                        r[4],
                        irradiance_wm2
                        )
                c.execute("""
                          INSERT INTO event (
                              ref, 
                              file_id, 
                              event_start, 
                              event_end, 
                              event_duration, 
                              anemometer_hz_1, 
                              anemometer_hz_2, 
                              irradiance_v,
                              windspeed_ms_1, 
                              windspeed_ms_2, 
                              wind_direction,
                              irradiance_wm2
                          )
                          VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                          """, subs)
                        
                prev_ts = ts
            except Exception as e:
                log.warning('Failed to add event: %s / %s' % (type(e), e))
            c.execute("""UPDATE raw_data SET processed = 1 WHERE rowid = ?""", (r[7],))
        c.execute('commit')

    def get_calibration(self):
        """
        Return a dict of dicts containing the calibration data for all sensors.

        Structure: { 'REF1': { CALIB },
                     'REF2': { CALIB },
                     .... }
        Where: CALIB is a dict with the following keys:
              ref
              anemometer_1_factor
              anemometer_2_factor
              max_windspeed_ms
              irradiance_factor
              max_irradiance
        """
        c = self._conn.cursor()
        c.execute("""SELECT * FROM calibration""")
        ret = dict()
        for r in c.fetchall():
            ret[r[0]] = {'ref': r[0],
                         'anemometer_1_factor': r[1],
                         'anemometer_2_factor': r[2],
                         'max_windspeed_ms': r[3],
                         'irradiance_factor': r[4],
                         'max_irradiance': r[5]}
        return ret

    def reset(self):
        """Reset the database to a clean state"""
        for table in ['event', 'raw_data', 'input_file', 'field_mapping', 'calibration', 'winda_schema_v_1_00']:
            self._conn.execute("""DROP TABLE %s""" % table)
        self.create_schema()

