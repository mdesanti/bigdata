%default PIGGYBANK_PATH '/home/hadoop/pig-0.11.1/contrib/piggybank/java/piggybank.jar'
%default FLIGHTS_PATH '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data/'
%default AIRPORTS_HBASE_PATH 'hbase://itba_tp1_airports'
%default PROTOBUF_PATH '/home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar'
%default OUTPUT_PATH 'metric1/output'

REGISTER '$PIGGYBANK_PATH';
REGISTER '$PROTOBUF_PATH';

flights = LOAD '$FLIGHTS_PATH' 
          USING org.apache.pig.piggybank.storage.CSVLoader() 
          AS (Year:chararray, Month:chararray, DayofMonth:chararray, DayOfWeek:chararray, 
              DepTime:chararray, CRSDepTime:chararray, ArrTime:chararray, CRSArrTime:chararray, 
              UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,  ActualElapsedTime:chararray,
              CRSElapsedTime:chararray, AirTime:chararray, ArrDelay:chararray, DepDelay:long, origin:chararray, 
              Dest:chararray, Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:chararray, 
              CancellationCode:chararray, Diverted:chararray, CarrierDelay:chararray, WeatherDelay:chararray,
              NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

airports = LOAD '$AIRPORTS_HBASE_PATH'
           USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:airport', '-loadKey true')
           AS (id:chararray, airport:chararray);

simple_flights = FOREACH flights GENERATE Year, origin, DepDelay;

joined = JOIN simple_flights BY origin, airports BY id;

grouped = GROUP joined BY (origin, Year, airport, id);

summed = FOREACH grouped GENERATE group.airport, group.Year, SUM(joined.DepDelay) AS totaldelay:long;

by_year = GROUP summed BY Year;

results = FOREACH by_year {
  sorted = ORDER summed BY totaldelay desc;
  top_5 = LIMIT sorted 5;
  generate group, flatten(top_5);
};

simple_results = FOREACH results GENERATE Year, airport, totaldelay;

STORE simple_results into '$OUTPUT_PATH' USING PigStorage (';');

 
