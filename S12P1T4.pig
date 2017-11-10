REGISTER '/home/acadgild/pig-0.14.0/lib/piggybank.jar' ;

crime_data = LOAD '/home/acadgild/Kushal/Session12/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER') ;

qualifying_data = foreach crime_data generate ToDate($2,'M/d/yyyy HH:mm:ss a') as (casedate:DateTime), (chararray)$8 as arrests ;

filter_data = filter qualifying_data by arrests == 'true' AND casedate >= ToDate('10/01/2014','M/d/yyyy') AND casedate <= ToDate('10/31/2015','M/d/yyyy') ;

group_qualifying_data = GROUP filter_data ALL ;

count_qualifying_data = foreach group_qualifying_data GENERATE COUNT(filter_data) ;

dump count_qualifying_data ;
