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

public class TakeOffDelayTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() throws IOException {
		TakeOffDelayMapper mapper = new TakeOffDelayMapper();
		TakeOffDelayReducer reducer = new TakeOffDelayReducer();
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
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("20"));
		values.add(new Text("5"));
		values.add(new Text("NA"));
		values.add(new Text("15"));
		values.add(new Text("-4"));
		reduceDriver.withInput(new Text("CA-April"), values);
		reduceDriver.withOutput(new Text("CA-April"), new DoubleWritable(9));
		reduceDriver.runTest();
	}
}

