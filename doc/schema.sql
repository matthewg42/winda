-- ==> D150530.CSV <==
-- Ref, Date, Time, RPM, Wind, Direction, Batt V, Ext V, Current
-- 01,30-05-2015,00:00:04,0,0,N,3.99,0.00,-200.87
-- 01,30-05-2015,00:01:04,0,0,N,3.99,0.00,-200.85
-- 01,30-05-2015,00:02:04,0,0,N,3.97,0.00,-200.87
-- 
-- ==> D160112.CSV <==
-- Ref, Date, Time, Wind 1, Wind 2, Direction, Irradiance Wm-2, Batt V
-- BB,12-01-2016,19:34:10,0,0,W,0.00,4.72
-- BB,12-01-2016,19:34:10,0,0,W,0.00,4.72
-- BB,12-01-2016,19:34:15,0,0,W,0.00,4.70

CREATE TABLE calibration (
	ref VARCHAR(12) PRIMARY KEY,
	anemometer_1_factor FLOAT,
	anemometer_2_factor FLOAT,
	max_windspeed_ms FLOAT,
	irradiance_factor FLOAT,
	max_irradiance FLOAT
);

CREATE TABLE field_mapping (
	header VARCHAR(128) UNIQUE,
	field VARCHAR(128) NOT NULL
);

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
	processed BOOLEAN
);

CREATE TABLE input_file (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path VARCHAR(255),
	import_date FLOAT,
	records INTEGER,
	errors INTEGER
);

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
	FOREIGN KEY(file_id) REFERENCES files(id)
);

-- Pre-populate calibrations for known devices
INSERT INTO calibration (ref,  anemometer_1_factor, anemometer_2_factor, max_windspeed_ms, irradiance_factor, max_irradiance)
VALUES                  ('BB', 1.42,                1.42,                100,              1.0,               1500);

-- Populate field mappings
-- Note: this method is MUCH faster than having a separate 
--       select for each record, at the cost of a little "wha?"
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
        UNION ALL SELECT 'battery_volts', 'batt_v';

-- The presence of this table can be used to check for compatibility and
-- completeness of the schema.  This should be the last statement
-- executed when creating the schema.
CREATE TABLE winda_schema_v_1_00 (id INT UNIQUE);

