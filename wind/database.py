"""
Provide database functionality using sqlite3
"""

import sqlite3
import os
import logging

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
        if not self.schema_exists():
            self.create_schema()

    def schema_exists(self):
        c = self._conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE name LIKE 'winda_schema_v_%';")
        if len(c.fetchall()) == 0:
            return False
        else:
            return True

    def create_schema(self):
        """ Create a new, empty database at the specified path. """
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
                      field VARCHAR(128) NOT NULL
                  )
                  """)

        log.debug('creating table input_file...')
        c.execute("""
                  CREATE TABLE input_file (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      path VARCHAR(255),
                      import_date FLOAT,
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
                      FOREIGN KEY(file_id) REFERENCES input_file(id)
                  )
                  """)

        log.debug('creating table events...')
        c.execute("""
                  CREATE TABLE events (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      ref VARCHAR(12),
                      file_id INTEGER,
                      event_start FLOAT,
                      event_end FLOAT,
                      event_duration FLOAT,
                      anemometer_hz_1 INTEGER,
                      anemometer_hz_2 INTEGER,
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
                      SELECT 'wind' AS 'header', 'wind_1' AS 'field'
                      UNION ALL SELECT 'wind speed', 'wind_1'
                      UNION ALL SELECT 'wind_speed', 'wind_1'
                      UNION ALL SELECT 'windspeed', 'wind_1'
                      UNION ALL SELECT 'wind ticks', 'wind_1'
                      UNION ALL SELECT 'wind_ticks', 'wind_1'
                      UNION ALL SELECT 'windticks', 'wind_1'
                      UNION ALL SELECT 'wind pulses', 'wind_1'
                      UNION ALL SELECT 'wind_pulses', 'wind_1'
                      UNION ALL SELECT 'windpulses', 'wind_1'
                      UNION ALL SELECT 'anemometer', 'wind_1'
                      UNION ALL SELECT 'anemometer hz', 'wind_1'
                      UNION ALL SELECT 'anemometer_hz', 'wind_1'
                      UNION ALL SELECT 'anemometerhz', 'wind_1'
                      UNION ALL SELECT 'ticks', 'wind_1'
                      UNION ALL SELECT 'pulses', 'wind_1'
                      UNION ALL SELECT 'wind 1', 'wind_1'
                      UNION ALL SELECT 'wind_1', 'wind_1'
                      UNION ALL SELECT 'wind1', 'wind_1'
                      UNION ALL SELECT 'wind speed 1', 'wind_1'
                      UNION ALL SELECT 'wind_speed_1', 'wind_1'
                      UNION ALL SELECT 'windspeed1', 'wind_1'
                      UNION ALL SELECT 'wind ticks 1', 'wind_1'
                      UNION ALL SELECT 'wind_ticks_1', 'wind_1'
                      UNION ALL SELECT 'windticks1', 'wind_1'
                      UNION ALL SELECT 'wind pulses 1', 'wind_1'
                      UNION ALL SELECT 'wind_pulses_1', 'wind_1'
                      UNION ALL SELECT 'windpulses1', 'wind_1'
                      UNION ALL SELECT 'anemometer 1', 'wind_1'
                      UNION ALL SELECT 'anemometer_1', 'wind_1'
                      UNION ALL SELECT 'anemometer1', 'wind_1'
                      UNION ALL SELECT 'anemometer hz 1', 'wind_1'
                      UNION ALL SELECT 'anemometer_hz_1', 'wind_1'
                      UNION ALL SELECT 'anemometerhz1', 'wind_1'
                      UNION ALL SELECT 'ticks 1', 'wind_1'
                      UNION ALL SELECT 'ticks_1', 'wind_1'
                      UNION ALL SELECT 'ticks1', 'wind_1'
                      UNION ALL SELECT 'pulses 1', 'wind_1'
                      UNION ALL SELECT 'pulses_1', 'wind_1'
                      UNION ALL SELECT 'pulses1', 'wind_1'
                      UNION ALL SELECT 'wind 2', 'wind_2'
                      UNION ALL SELECT 'wind_2', 'wind_2'
                      UNION ALL SELECT 'wind2', 'wind_2'
                      UNION ALL SELECT 'wind speed 2', 'wind_2'
                      UNION ALL SELECT 'wind_speed_2', 'wind_2'
                      UNION ALL SELECT 'windspeed2', 'wind_2'
                      UNION ALL SELECT 'wind ticks 2', 'wind_2'
                      UNION ALL SELECT 'wind_ticks_2', 'wind_2'
                      UNION ALL SELECT 'windticks2', 'wind_2'
                      UNION ALL SELECT 'wind pulses 2', 'wind_2'
                      UNION ALL SELECT 'wind_pulses_2', 'wind_2'
                      UNION ALL SELECT 'windpulses2', 'wind_2'
                      UNION ALL SELECT 'anemometer 2', 'wind_2'
                      UNION ALL SELECT 'anemometer_2', 'wind_2'
                      UNION ALL SELECT 'anemometer2', 'wind_2'
                      UNION ALL SELECT 'anemometer hz 2', 'wind_2'
                      UNION ALL SELECT 'anemometer_hz_2', 'wind_2'
                      UNION ALL SELECT 'anemometerhz2', 'wind_2'
                      UNION ALL SELECT 'irradiance', 'irradiance'
                      UNION ALL SELECT 'irradiance v', 'irradiance'
                      UNION ALL SELECT 'irradiance_v', 'irradiance'
                      UNION ALL SELECT 'irr', 'irradiance'
                      UNION ALL SELECT 'irradiance wm-2', 'irradiance'
                      UNION ALL SELECT 'irradiance_wm-2', 'irradiance'
                      UNION ALL SELECT 'irradiancewm-2', 'irradiance'
                      UNION ALL SELECT 'irradiancewm2', 'irradiance'
                      UNION ALL SELECT 'irradiance wm2', 'irradiance'
                      UNION ALL SELECT 'irradiance_wm2', 'irradiance'
                      UNION ALL SELECT 'batt', 'batt_v'
                      UNION ALL SELECT 'batt_v', 'batt_v'
                      UNION ALL SELECT 'batt v', 'batt_v'
                      UNION ALL SELECT 'battery', 'batt_v'
                      UNION ALL SELECT 'battery v', 'batt_v'
                      UNION ALL SELECT 'battery_v', 'batt_v'
                      UNION ALL SELECT 'batt_volts', 'batt_v'
                      UNION ALL SELECT 'batt volts', 'batt_v'
                      UNION ALL SELECT 'battery volts', 'batt_v'
                      UNION ALL SELECT 'battery_volts', 'batt_v'
                  """)
        
        log.debug('creating table winda_schema_v_1_00...')
        c.execute("""CREATE TABLE winda_schema_v_1_00 (id INT UNIQUE)""")

    def info(self):
        """ Return a dict with some helpful information about the database """
        d = dict()
        d['Database file'] = self._path
        d['Size'] = os.path.getsize(self._path)
        c = self._conn.cursor()
        c.execute("""SELECT DISTINCT id from input_file""")
        d['Number of files added'] = len(c.fetchall())
        c.execute("""SELECT 1 FROM events""")
        d['Number of records'] = len(c.fetchall())
        return d

    def add(self, *args):
        """ Add a list of glob patterns or files to the datbase """
        raise(Exception('TODO: Database.add(%s)' % args))
        
    def add_pattern(self, pattern):
        """ Add a single glob pattern of files to the datbase """
        raise(Exception('TODO: Database.add_pattern()'))
        

    def add_file(self, path):
        """ Add a single CSV file to the database """
        raise(Exception('TODO: Database.add_file()'))


