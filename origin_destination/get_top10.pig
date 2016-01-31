
/*******************************************************************************
* get_top_10.pig: Get top 10 airports from filtered data                       *
*******************************************************************************/

/* registering piggybank CSV storage:
 http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/ec2-user/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

/* load data from filtered dataset */
not_empty = LOAD '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (Origin:chararray, OriginCountry:chararray, OriginStateName:chararray, Dest:chararray, DestCountry:chararray, DestStateName:chararray);

/* Group by origin and destination */
Origin = GROUP not_empty BY Origin;
Destination = GROUP not_empty BY Dest;

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

/* get and dump the top 10 airports */
TOP_10 = LIMIT Popular 10;
DUMP TOP_10;
