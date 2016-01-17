The following is adapted from an email from Dr. Little, dated 2016-01-09.

## Inputs

*   Data is to be read from files containing records in CSV format and delimited by some combination of carriage returns and/or line feeds
*   The first line of the file *may* contain field names, which will be something like this: "Ref, Date, Time, RPM, Wind, Direction, Batt V, Ext V, Current"
    *   *Ref* is some string identified, e.g. *01*
    *   *Date* is the date for this record in DD-MM-YYYY format, e.g. *28-05-2015*
    *   *Time* is the time for this record in HH:MM:SS format, e.g. *00:01:04*
    *   *RPM* is the revolutions per minute (TODO: of an anemometer?), e.g. *0*
    *   *Wind* is the wind speed in (TODO: units == m/s?), e.g. *0*
    *   *Direction* is the wind direction, which is one of N, S, E, W (TODO: others?) e.g. *N*
    *   *Batt V* is the battery voltage in (TODO: units?) e.g. *4.06*
    *   *Ext V* is (TODO: external voltage? Units?) e.g. *0.00*
    *   *Current* is (TODO: current of what? Units?) e.g. *-198.20*
*   An example record:

    01,28-05-2015,00:01:04,0,0,N,4.06,0.00,-198.20

*   The data rate should be one record per minute, but this should not be assumed - it may vary
*   One file is produced for each calendar day, the file name contains the date (TODO: format of filename & example?)

## Outputs

### Accumulate data into a single data source

A single "mega file" with all data from all CSV input files ever processed should be maintained.  

## Wind Probability Distribution (Windspeed Transform)

In this case the number of times the wind is at (say) 3m/s is added up and given as a percentage of the total number of data points.  This needs to be done for wind speeds from 0-40m/s (or higher) in increments of 0.5m/s (eg if a wind speed is 10.4m/s it will go to the 10-10.5 m/s 'bin'.  I'd like this outputted as a .csv file with wind speeds and probability. This will turn the data into a much smaller amount.

## Wind Speed With Wind Direction (extra - nice to have)

This means taking all the wind speed data and putting all the data from each direction into a smaller data subset. Each direction would be analysed as per section 2, so that probability distributions can be generated for each direction.


# Proposed Implementation

A single Python program named "winda.py" which will maintain a database of records added so far.  The program is command-line based, with multiple sub-command in the style of git. The syntax for the program is as follows assuming the command prompt current working directory is where the program is installed):

*nix systems:

    ./winda.py command [command-args]

Windows:

    winda command [command-args]

For example:

    winda add *.CSV


## Synopsis

Some example commands to give a quick overview of usage:

    winda add *.CSV
    winda show files
    winda show files P15*.CSV
    winda show calibration
    winda show calibration BB
    winda calibrate AA 1.42 1.44 100 1.0 1500
    winda speeds --date 2015-06-02 > June_2_speeds.csv
    winda speeds --date 2015-06-02 --dir > June_2_speeds_and_directions.csv
    winda speeds --file P150602.CSV > june_2_speeds.csv
    winda speeds --increment 5 --file P1506*.CSV > june_speeds_5ms_increments.csv
    winda speeds --range 0-30 --date 150601 > june_1_speeds_to_30_ms.csv
    winda speeds --range 20-40 --increment 0.25 --from 150601 --to 150630 > out.csv
    winda export > all_data.csv
    winda export --date 150601 > some_data.csv
    winda info
    winda reset

Still to implement: 

    winda remove --file P1506*.CSV
    winda remove --date 150601

Really useful:

    winda --help
    winda add --help
    winda show --help
    winda calibrate --help
    winda speeds --help
    winda export --help
    winda info --help
    winda reset --help

## Commands

### Command: add

#### Syntax

    winda add pattern [pattern ...]

#### Description

Add data file(s) to the database. Will not re-add a file which has already been added. Errors may be printed indicating trouble processing records from a file.  If no records were added to the database, the file is not considered to have been added, and another attempt may be made.

#### Command arguments

This command requires at least one argument - the name of a file to add, or a glob pattern which matches at least one file to add.

#### Example

    winda add *.CSV

### Command: show files

#### Syntax

    winda show files [pattern]

#### Description

Shows information about files which have been added to the database.

#### Command arguments

If a glob pattern is provided, only files matching that pattern are listed.

#### Examples

    winda show files
    winda show files P15*.CSV

### Command: show calibration

#### Syntax

    winda show calibration [ref]

#### Description

Shows information about calibration of sensors by ref value.

#### Command arguments

If ref is specified, only that sensor is listed in the output.

#### Examples

    winda show calibration 
    winda show calibration BB

### Command: remove

NOTE: not implemented yet

#### Syntax

    winda remove|rm <filter>

#### Description

Remove data from from the database according to some filter critera. See section "Data Filters" below for how to use a filter. Note that when removing by date or date range, the file will only be removed once all records in event and raw\_data have been removed for that file.

#### Command arguments

The remove command must be supplied with a valid data filter. See section "Data Filters" below for how to use a filter.

#### Examples

    winda remove --file P1506*.CSV
    winda remove --date 150601
    winda remove --from 150601 --to 150701

### Command: speeds

#### Syntax

    winda speeds [--range r] [--increment i] [--direction-split] <filter>

#### Description

Analyse data from the database, outputting the wind speed transform for the specified records.  The default range is 0-40 m/s, and the default increment is 0.5 m/s.

#### Command arguments

The speeds command must be supplied with a valid data filter. See section "Data Filters" below for how to use a filter.  

The command may also take an optional "--range r" value which specifies the range of wind speeds to be output.  The --range parameter, *r* is a pair of integers (the lower and upper bound of the range), separated by a "-".  For example:

    --range 0-30

...means to output for the range of speeds 0 m/s to 30 m/s inclusive.

The command may also take an optional "--increment i" value which specifies the increment used to split the output.  The --increment parameter, *i* is a decimal value which defines the step size, for example:

    --increment 1.5

...to output in 1.5 m/s steps.

The command may also take an optional [--direction-split] argument, which will do one analysis per wind direction in the input data.  The output will have an addition column with the windo direction in it.

#### Examples

    winda speed --all
    winda speed --increment 5 --file P1506*.CSV
    winda speed --date 150601
    winda speed --range 20-40 --increment 0.25 --from 150601 --to 150701

### Command: export

#### Syntax

    winda export <filter> 

#### Description

Dump data from the database to stadard output in CSV format.  The following fields are included:

*   ref
*   event\_start
*   event\_end
*   windspeed\_ms\_1
*   windspeed\_ms\_2
*   wind\_direction
*   irradiance\_wm2

#### Command Arguments

This command may take one or more filter arguments to limit the output to just records matching those arguments.  If no filter arguments are provided, all data is exported.

#### Example

    winda export dump.csv

### Command: reset

#### Syntax

    winda reset

#### Description

Clear the database (prompts for confirmation).

#### Command Arguments

This command does not take any arguments.

#### Example

    winda reset

### Command: info

#### Syntax

    winda info

#### Description

Show summary information about the database.  Output includes the database filename, the number of files which have been added and the number of records in the database:

    Database file:          all.db
    Size:                   13,456 bytes
    Number of files added:  3
    Number of records:      4320

#### Command Arguments

This command does not take any arguments.

#### Example

    winda info

## Data Filters

Several arguments can be used to specify data from the database in different ways:

### By File

To specify data from specific files, use the --file option:

    --file pattern

...where *pattern* is a glob pattern which matches one or more files which have been added to the database.

### By Date

To specify data from a specific date, use the --date option:

    --date YYMMDD

...where *YYMMDD* is the date from which records are to be selected in YYMMDD format.  For example:

    --date 150503

...to select all records from the 3rd of May 2015.

### By Date/Time Range

To specify data from a period of time, use --from and --to:

    --from YYMMDD[HHMMSS] --to YYMMDD[HHMMSS]

Both the --from and --to allow the specification of a date/time. If the time is not required, inly the date may be specified. For example:

    --from 150501 --to 150502133000

...which will select records from the 1st of May 2015 through 1:30 pm on the 2nd of May 2015.


