
/*******************************************************************************
* Read Origin and destination dataset with pig                                 *
*******************************************************************************/

raw_data = LOAD '$input' USING PigStorage(',') AS (ItinID,MktID,SeqNum,Coupons,Year,Quarter,Origin:chararray,OriginAptInd,OriginCityNum,OriginCountry,OriginStateFips,OriginState,OriginStateName,OriginWac,Dest:chararray,DestAptInd,DestCityNum,DestCountry,DestStateFips,DestState,DestStateName,DestWac,Break,CouponType,TkCarrier,OpCarrier,RPCarrier,Passengers,FareClass,Distance,DistanceGroup,Gateway,ItinGeoType,CouponGeoType);

packed_data = FOREACH raw_data GENERATE Origin, OriginCountry, OriginStateName, Dest, DestCountry, DestStateName;

/* removing empty values */
A = FILTER packed_data BY Origin IS NOT NULL ;
B = FILTER A BY Dest IS NOT NULL ;
C = FILTER B BY Origin != '""' ;
not_empty = FILTER C BY Dest != '""' ;

/* Dump filtered data in HDFS */
STORE not_empty INTO '$filtered';

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

TOP_10 = LIMIT Popular 10;
DUMP TOP_10;

/* store data to HDFS */
STORE TOP_10 INTO '$output';
