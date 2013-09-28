package ar.edu.itba.it.bigdata.mapreduce.flightHours;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FlightHoursReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private Logger logger = Logger.getLogger("Flight Hours Mapper");

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws InterruptedException {

		double count = 0;
		for (DoubleWritable value : values) {
			count += value.get();
		}
		
		try {
			context.write(key, new DoubleWritable(count));
		} catch (IOException e) {
			logger.log(Level.ERROR, "IOException while writing output to context");
		}
	}
}