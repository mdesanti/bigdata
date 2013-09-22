package ar.edu.itba.it.bigdata.mapreduce.flightHours;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightHours {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: MaxTemperature <input path> <outputpath>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(FlightHours.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.getConfiguration().set("planeType", args[2]);

		job.setMapperClass(FlightHoursMapper.class);
		job.setReducerClass(FlightHoursReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
