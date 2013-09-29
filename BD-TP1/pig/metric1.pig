REGISTER ./pig-0.11.1/contrib/piggybank/java/piggybank.jar;

flights = LOAD '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data' 
          USING org.apache.pig.piggybank.storage.CSVLoader() 
          AS (Year:chararray, Month:chararray, DayofMonth:chararray, DayOfWeek:chararray, DepTime:chararray, CRSDepTime:chararray, 
              ArrTime:chararray, CRSArrTime:chararray, UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray, 
              ActualElapsedTime:chararray, CRSElapsedTime:chararray, AirTime:chararray, ArrDelay:chararray, 
              DepDelay:chararray, origin:chararray, Dest:chararray, Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:chararray, 
              CancellationCode:chararray, Diverted:chararray, CarrierDelay:chararray, WeatherDelay:chararray, 
              NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

airports = LOAD 'hbase://itba_tp1_airports' 
           USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:airport', '-loadKey true') 
           AS (id:chararray, airport:chararray);


joined = JOIN flights BY origin, airports BY id;