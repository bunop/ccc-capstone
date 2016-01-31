
/********************************************************************************
* top10 airline: Read Origin and destination dataset with pig and rank the      *
* top 10 airlines by on-time arrival performance.                               *
********************************************************************************/

/* registering piggybank CSV storage:
 http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/ec2-user/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

/* load data from filtered dataset */
filtered = LOAD '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (FlightDate:chararray,AirlineID:chararray,FlightNum:int,Origin:chararray,OriginCityName:chararray,OriginStateName:chararray,Dest:chararray,DestCityName:chararray,DestStateName:chararray,CRSDepTime:chararray,DepDelay:float,CRSArrTime:chararray,ArrDelay:float,Cancelled:int,CancellationCode:chararray,Diverted:int,CRSElapsedTime:float,ActualElapsedTime:float,AirTime:float,Distance:float);

/* avoid cancelled or diverted flight */
arrived_flights = FILTER filtered BY Cancelled == 0 AND Diverted == 0;

/* strip columns */
flights = FOREACH arrived_flights GENERATE AirlineID, ArrDelay;

/* grop by path and AirlineID */
airlines = GROUP flights BY (AirlineID);

/* calculate average */
airline_delays = FOREACH airlines GENERATE FLATTEN($0), AVG(flights.ArrDelay) AS avg_delay;

/* order by counts - using PARALLEL reduce tasks */
airline_ranked = ORDER airline_delays BY avg_delay ASC;

/* get and dump the top 10 airlines */
TOP_10 = LIMIT airline_ranked 10;
DUMP TOP_10;
