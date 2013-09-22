package ar.edu.itba.it.bigdata.mapreduce.delay;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TakeOffDelayReducer extends
		Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		int times = 0;
		for (Text value : values) {
			try {
				Integer delay = Integer.parseInt(value.toString());
				count += delay;
				times++;
			} catch(NumberFormatException e) {
				//do nothing, some values can be "NA"
			}
		}
		
		if(times == 0 ) {
			context.write(key, new DoubleWritable(0));
		} else {
			context.write(key, new DoubleWritable(count / (double)times));
		}
	}
}