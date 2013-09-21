package ar.edu.itba.it.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TakeOffDelay {

	public static HashMap<String, String> airportsHashTable;

	public static class TakeOffDelayMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] flightInformation = value.toString().split(",");
			String IATA_code = flightInformation[16];
			String airportInformation = airportsHashTable.get(IATA_code);
			String airportFields[] = airportInformation.split(",");
			String state = airportFields[4];
			context.write(new Text(state), new Text(value.toString()));

		}
	}

	public static class TakeOffDelayReducer extends
			Reducer<LongWritable, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			int times = 0;
			for (Text value : values) {
				String flightInformation[] = value.toString().split(",");
				String departureDelay = flightInformation[15];
				count += Integer.parseInt(departureDelay);
				times++;
			}

			context.write(key, new DoubleWritable(count / times));
		}
	}

	public static void configure(JobConf conf) {
		// Read the broadcasted file
		File file = new File(conf.get("airports.csv"));
		// Hashtable to store the tuples
		airportsHashTable = new HashMap<String, String>();
		BufferedReader br = null;
		String line = null;
		try {
			br = new BufferedReader(new FileReader(file));
			while ((line = br.readLine()) != null) {
				String airportInformation[] = line.split(",", 2);
				// Insert into Hashtable
				String IATA_code = airportInformation[0];
				airportsHashTable.put(IATA_code, airportInformation[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperature <input path> <outputpath>");
			System.exit(-1);
		}
		Job job = new Job();
		JobConf conf = new JobConf(job.getConfiguration());
		configure(conf);
		job.setJarByClass(TakeOffDelay.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TakeOffDelayMapper.class);
		job.setReducerClass(TakeOffDelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
