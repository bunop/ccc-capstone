
/*******************************************************************************
* load_origin_destination.pig: Read Origin and destination dataset with pig    *
*******************************************************************************/

/* registering piggybank CSV storage:
 http://stackoverflow.com/questions/17816078/csv-reading-in-pig-csv-file-contains-quoted-comma
*/
REGISTER '/home/ec2-user/capstone/piggy_bank/contrib/piggybank/java/piggybank.jar';

raw_data = LOAD '$input' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (ItinID,MktID,SeqNum,Coupons,Year,Quarter,Origin:chararray,OriginAptInd,OriginCityNum,OriginCountry:chararray,OriginStateFips,OriginState,OriginStateName:chararray,OriginWac,Dest:chararray,DestAptInd,DestCityNum,DestCountry:chararray,DestStateFips,DestState,DestStateName:chararray,DestWac,Break,CouponType,TkCarrier,OpCarrier,RPCarrier,Passengers,FareClass,Distance,DistanceGroup,Gateway,ItinGeoType,CouponGeoType);

packed_data = FOREACH raw_data GENERATE Origin, OriginCountry, OriginStateName, Dest, DestCountry, DestStateName;

/* there are header inside raw_data, filter out them*/
filtered_data = FILTER raw_data BY Origin != 'Origin' OR Dest != 'Dest' ;

/* removing empty values */
A = FILTER filtered_data BY Origin IS NOT NULL ;
B = FILTER A BY Dest IS NOT NULL ;
C = FILTER B BY Origin != '""' ;
not_empty = FILTER C BY Dest != '""' ;

/* Dump filtered data in HDFS */
STORE not_empty INTO '$filtered' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_OUTPUT_HEADER');
