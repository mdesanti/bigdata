package ar.edu.itba.it.bigdata.mapreduce.flownMiles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlownMilesReducer extends
		Reducer<Text, IntWritable, Text, LongWritable> {
	

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		long count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}

		System.out.println("Writing " + key + " -> " + count);
		context.write(key, new LongWritable(count));
	}
}