REGISTER '/home/acadgild/pig-0.14.0/lib/piggybank.jar';

crime_data = LOAD '/home/acadgild/Kushal/Session12/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

qualifying_data = foreach crime_data generate (chararray)$0 as caseid, (chararray)$14 as fbicode;

group_qualifying_data = GROUP qualifying_data by fbicode ;

count_qualifying_data = foreach group_qualifying_data GENERATE group,COUNT(qualifying_data) ;

dump count_qualifying_data ;
