package ar.edu.itba.it.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TakeOffDelayReducer extends
		Reducer<LongWritable, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		int times = 0;
		for (Text value : values) {
			Integer delay = Integer.parseInt(value.toString());
			count += delay;
			times++;
		}

		context.write(key, new DoubleWritable(count / times));
	}
}