REGISTER '/home/acadgild/pig-0.14.0/lib/piggybank.jar' ;

crime_data = LOAD '/home/acadgild/Kushal/Session12/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

qualifying_data = foreach crime_data generate (chararray)$5 as crimetype, (chararray)$8 as arrests, (chararray)$11 as district ;

filter_data = filter qualifying_data by crimetype == 'THEFT' AND arrests == 'true' ;

group_qualifying_data = GROUP filter_data by district ;

count_qualifying_data = foreach group_qualifying_data GENERATE group,COUNT(filter_data) ;

dump count_qualifying_data ;
