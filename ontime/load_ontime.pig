
/*******************************************************************************
* load_origin_ontime.pig: Read ontime dataset with pig                         *
*******************************************************************************/

/* registering piggybank CSV storage:
http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/paolo/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

/* load data from HDFS. CRS means Computerized Reservations Systems*/
/* skip header strings: http://stackoverflow.com/questions/19115298/skiping-the-header-while-loading-the-text-file-using-piglatin */
raw_data = LOAD '$input' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate:chararray,UniqueCarrier,AirlineID:chararray,Carrier,TailNum,FlightNum:int,Origin:chararray,OriginCityName:chararray,OriginState,OriginStateFips,OriginStateName:chararray,OriginWac,Dest:chararray,DestCityName:chararray,DestState,DestStateFips,DestStateName:chararray,DestWac,CRSDepTime:chararray,DepTime,DepDelay:float,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime:chararray,ArrTime,ArrDelay:float,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled:int,CancellationCode:chararray,Diverted:int,CRSElapsedTime:float,ActualElapsedTime:float,AirTime:float,Flights,Distance:float,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,FirstDepTime,TotalAddGTime,LongestAddGTime,DivAirportLandings,DivReachedDest,DivActualElapsedTime,DivArrDelay,DivDistance,Div1Airport,Div1WheelsOn,Div1TotalGTime,Div1LongestGTime,Div1WheelsOff,Div1TailNum,Div2Airport,Div2WheelsOn,Div2TotalGTime,Div2LongestGTime,Div2WheelsOff,Div2TailNum);

/* there are header inside raw_data, filter out them*/
filtered_data = FILTER raw_data BY FlightDate != 'FlightDate' OR AirlineID != 'AirlineID' ;

packed_data = FOREACH filtered_data GENERATE FlightDate, AirlineID, FlightNum, Origin, OriginCityName, OriginStateName, Dest, DestCityName, DestStateName, CRSDepTime,  DepDelay, CRSArrTime, ArrDelay, Cancelled, CancellationCode, Diverted, CRSElapsedTime, ActualElapsedTime, AirTime, Distance;

/* Dump filtered data in HDFS */
STORE packed_data INTO '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_OUTPUT_HEADER');
