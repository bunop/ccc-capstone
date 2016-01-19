
/*******************************************************************************
* load_origin_ontime.pig: Read ontime dataset with pig                         *
*******************************************************************************/

/* load data from HDFS. CRS means Computerized Reservations Systems*/

raw_data = LOAD '$input' USING PigStorage(',') AS (Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate:datetime,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,Origin:chararray,OriginCityName:chararray,OriginState,OriginStateFips,OriginStateName:chararray,OriginWac,Dest:chararray,DestCityName:chararray,DestState,DestStateFips,DestStateName:chararray,DestWac,CRSDepTime:chararray,DepTime,DepDelay:float,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime:chararray,ArrTime,ArrDelay:float,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled:boolean,CancellationCode:chararray,Diverted:boolean,CRSElapsedTime:float,ActualElapsedTime:float,AirTime:float,Flights,Distance:float,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,FirstDepTime,TotalAddGTime,LongestAddGTime,DivAirportLandings,DivReachedDest,DivActualElapsedTime,DivArrDelay,DivDistance,Div1Airport,Div1WheelsOn,Div1TotalGTime,Div1LongestGTime,Div1WheelsOff,Div1TailNum,Div2Airport,Div2WheelsOn,Div2TotalGTime,Div2LongestGTime,Div2WheelsOff,Div2TailNum);

packed_data = FOREACH raw_data GENERATE FlightDate, Origin, OriginCityName, Dest, DestCityName ;
