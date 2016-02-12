
/*******************************************************************************
* get_popolarity.pig: Get airports popoluarity from filtered data - ontime     *
*******************************************************************************/

/* registering piggybank CSV storage:
 http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/paolo/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

/* load data from filtered dataset */
filtered = LOAD '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (FlightDate:chararray,AirlineID:chararray,FlightNum:int,Origin:chararray,OriginCityName:chararray,OriginStateName:chararray,Dest:chararray,DestCityName:chararray,DestStateName:chararray,CRSDepTime:chararray,DepDelay:float,CRSArrTime:chararray,ArrDelay:float,Cancelled:int,CancellationCode:chararray,Diverted:int,CRSElapsedTime:float,ActualElapsedTime:float,AirTime:float,Distance:float);

/* Group by origin and destination */
Origin = GROUP filtered BY Origin;
Destination = GROUP filtered BY Dest;

/* Count occurencies */
Origin_counts = FOREACH Origin GENERATE FLATTEN($0) AS airport, COUNT($1) AS count;
Destination_counts = FOREACH Destination GENERATE FLATTEN($0) AS airport, COUNT($1) AS count;

/* Put results in the same table */
United_counts = UNION Origin_counts, Destination_counts;

/* Group by airport code */
United_group = GROUP United_counts BY airport;

/* Sum occurrences */
Total_counts = FOREACH United_group GENERATE FLATTEN($0), SUM(United_counts.count) AS count;

/* order by counts - using PARALLEL reduce tasks */
Popular = ORDER Total_counts BY count DESC PARALLEL 4;

-- http://stackoverflow.com/questions/22987036/usage-of-apache-pig-rank-function
Ranked = RANK Popular BY count DESC;

/* get and dump the top 10 airports */
TOP_10 = LIMIT Popular 10;
DUMP TOP_10;

/* Dump filtered data in HDFS */
STORE Ranked INTO '$popular' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_OUTPUT_HEADER');
