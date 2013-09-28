package ar.edu.itba.it.bigdata.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ar.edu.itba.it.bigdata.mapreduce.cancelledFlights.CancelledFlightsMapper;
import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesMapper;

public class App {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
		final AppConfig config;

        config = CLIParser.getAppConfig(args);

        if (config == null) {
            return;
        }
        
        
		Job job = new Job();
		
		Configuration conf = job.getConfiguration();

		FileInputFormat.addInputPath(job, new Path(config.getInPath()));
		FileOutputFormat.setOutputPath(job, new Path(config.getOutPath()));
		
		job.setJarByClass(App.class);

		job.setMapperClass(config.getMapper());
		job.setReducerClass(config.getReducer());

		job.setMapOutputKeyClass(config.getMapOutputKeyClass());
		job.setMapOutputValueClass(config.getMapOutputValueClass());

		job.setOutputKeyClass(config.getOutputKeyClass());
		job.setOutputValueClass(config.getOutputValueClass());
		
		Map<String, String> extras = config.getExtras();
		
		if(config.getMapper().equals(CancelledFlightsMapper.class) || config.getMapper().equals(FlownMilesMapper.class)) {
			DistributedCache.addArchiveToClassPath(new Path(extras.get("carriersPath")), conf, FileSystem.get(conf));
		}
		
		for(String key: extras.keySet()) {
			conf.set(key, extras.get(key));
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
