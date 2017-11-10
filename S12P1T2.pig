REGISTER '/home/acadgild/pig-0.14.0/lib/piggybank.jar';

crime_data = LOAD '/home/acadgild/Kushal/Session12/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

dataset = foreach crime_data generate (chararray)$14 as fbicode;

elg_dataset = filter dataset  by fbicode == '26' ;

elg_dataset_groupall = group elg_dataset ALL ;

count_elg_dataset = foreach elg_dataset_groupall GENERATE COUNT(elg_dataset.fbicode) ;

dump count_elg_dataset ;
