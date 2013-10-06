package tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ar.edu.itba.it.bigdata.mapreduce.delay.TakeOffDelayMapper;
import ar.edu.itba.it.bigdata.mapreduce.delay.TakeOffDelayReducer;
import ar.edu.itba.it.bigdata.mapreduce.flightHours.FlightHoursMapper;
import ar.edu.itba.it.bigdata.mapreduce.flightHours.FlightHoursReducer;

public class FlightHoursTest {

	MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() throws IOException {
		FlightHoursMapper mapper = new FlightHoursMapper();
		FlightHoursReducer reducer = new FlightHoursReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		Configuration conf = new Configuration();
		mapDriver.setConfiguration(conf);
		File file = new File("src/test/resources/carriers/carriers.csv");
		DistributedCache.addArchiveToClassPath(
				new Path(file.getAbsolutePath()), conf, FileSystem.get(conf));
	}


	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(3.5));
		values.add(new DoubleWritable(12.3));
		values.add(new DoubleWritable(5.6));
		values.add(new DoubleWritable(8.2));
		values.add(new DoubleWritable(1.4));
		reduceDriver.withInput(new Text("N10156"), values);
		//this is not the ideal thing to test... there should be a way of comparing floating point results
		reduceDriver.withOutput(new Text("N10156"), new DoubleWritable(30.999999999999996));
		reduceDriver.runTest();
	}
}

