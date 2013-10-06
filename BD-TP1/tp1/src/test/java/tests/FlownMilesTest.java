package tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesMapper;
import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesReducer;

public class FlownMilesTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setUp() throws IOException {
		FlownMilesMapper mapper = new FlownMilesMapper();
		FlownMilesReducer reducer = new FlownMilesReducer();
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
	public void testAvailableDistance() {
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"1987,10,9,5,1606,1505,1708,1608,PS,1453,NA,62,63,NA,60,61,BUR,OAK,325,NA,NA,1,NA,1,NA,NA,NA,NA,NA"));
		mapDriver.withOutput(new Text("Pacific Southwest Airlines-1987"),
				new IntWritable(325));
		mapDriver.runTest();
	}

	@Test
	public void testNADistance() {
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"1987,10,9,5,1606,1505,1708,1608,PS,1453,NA,62,63,NA,60,61,BUR,OAK,NA,NA,NA,1,NA,1,NA,NA,NA,NA,NA"));
		mapDriver.withOutput(new Text("Pacific Southwest Airlines-1987"),
				new IntWritable(325));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(300));
		values.add(new IntWritable(220));
		values.add(new IntWritable(120));
		values.add(new IntWritable(200));
		values.add(new IntWritable(1000));
		reduceDriver.withInput(new Text("Pacific Southwest Airlines-1987"), values);
		reduceDriver.withOutput(new Text("Pacific Southwest Airlines-1987"), new LongWritable(1840));
		reduceDriver.runTest();
	}
}
