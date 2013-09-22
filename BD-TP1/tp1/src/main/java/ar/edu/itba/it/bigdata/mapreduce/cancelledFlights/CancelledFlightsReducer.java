package ar.edu.itba.it.bigdata.mapreduce.cancelledFlights;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancelledFlightsReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}

		System.out.println("Writing " + key + " -> " + count);
		context.write(key, new IntWritable(count));
	}
}