
/********************************************************************************
* top10 carriersbyairport: Read Origin and destination dataset with pig and     *
* rank the top-10 carriers in decreasing order of on-time departure performance *
* from X                                                                        *
********************************************************************************/

/* registering piggybank CSV storage:
 http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/paolo/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

/* load data from filtered dataset */
filtered = LOAD '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (FlightDate:chararray,AirlineID:chararray,FlightNum:int,Origin:chararray,OriginCityName:chararray,OriginStateName:chararray,Dest:chararray,DestCityName:chararray,DestStateName:chararray,CRSDepTime:chararray,DepDelay:float,CRSArrTime:chararray,ArrDelay:float,Cancelled:int,CancellationCode:chararray,Diverted:int,CRSElapsedTime:float,ActualElapsedTime:float,AirTime:float,Distance:float);

/* avoid cancelled or diverted flight */
arrived_flights = FILTER filtered BY Cancelled == 0 AND Diverted == 0;

/* strip columns */
flights = FOREACH arrived_flights GENERATE Origin, AirlineID, DepDelay;

/* grop by Origin and AirlineID */
carrier = GROUP flights BY (Origin, AirlineID);

/* calculate average */
carrier_delays = FOREACH carrier GENERATE group.Origin, group.AirlineID, AVG(flights.DepDelay) AS avg_delay;

/* group by airport X*/
origin_delays = GROUP carrier_delays BY Origin;

/* order by delay each origin */
carriers_by_airport = FOREACH origin_delays {
  sorted_origin = ORDER carrier_delays BY avg_delay ASC;
  top10 = LIMIT sorted_origin 10;
  GENERATE FLATTEN(top10);
};

/* write results to HDFS */
STORE carriers_by_airport INTO '$results' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_OUTPUT_HEADER');;

/* in cassandra?
REGISTER '/usr/share/cassandra/lib/apache-cassandra-2.0.17.jar';
STORE airports_by_airport INTO 'cql://capstone/airportsbyairport?output_query=update airportsbyairport set origin @ #,destination @ #,depdelay @ #' USING CqlStorage();
*/
