# crunch-csv-to-rcfile
Crunch CSV to RCFile example

This simple project demonstrates how to write out RCFile files using Apache Crunch, using the example of input CSV data.

Run the job with:

    hadoop jar crunchcsvtorcfile-0.0.1-SNAPSHOT-job.jar [numberofcolumnsinthedata] /your/path/to/input/csv/ /your/output/path/
    
Read the RCFile data in Hive with:

    CREATE EXTERNAL TABLE rc_output (field1name field1type, [etc.])
    STORED AS RCFILE
    LOCATION '/your/output/path/';
    
    SELECT * FROM rc_output;
