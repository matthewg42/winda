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

#### Development Note

It might be advantageous to keep all data in a DBM file indexed on the timestamp string in ISO format, or in a sqlite database indexed on the timestamp of a record. This would permit the following operations to be performed efficiently:

1.  Detection of mutliple values for a single timestamp (i.e. prevent re-processing of files)
2.  Creation of a single CSV file sorted by record date/time in an efficient manner, regardless of the order in which input files are added to the data set
3.  Extraction of chronological slices of data

## Wind Proability Distribution

In this case the number of times the wind is at (say) 3m/s is added up and given as a percentage of the total number of data points.  This needs to be done for wind speeds from 0-40m/s (or higher) in increments of 0.5m/s (eg if a wind speed is 10.4m/s it will go to the 10-10.5 m/s 'bin'.  I'd like this outputted as a .csv file with wind speeds and probability. This will turn the data into a much smaller amount.

## Wind Speed With Wind Direction (extra - nice to have)

This means taking all the wind speed data and putting all the data from each direction into a smaller data sub set. Each direction would be analysed as per section 2, so that probability distributions can be generated for each direction.

