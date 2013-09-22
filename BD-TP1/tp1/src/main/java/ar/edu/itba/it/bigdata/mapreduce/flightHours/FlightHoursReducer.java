package ar.edu.itba.it.bigdata.mapreduce.flightHours;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightHoursReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double count = 0;
		for (DoubleWritable value : values) {
			count += value.get();
		}
		
		context.write(key, new DoubleWritable(count));
	}
}