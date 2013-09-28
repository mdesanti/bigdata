package ar.edu.itba.it.bigdata.mapreduce.flownMiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlownMilesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private HashMap<String, String> carriersHashTable;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] flightInformation = value.toString().split(",");
		String carrierCode = flightInformation[8].replace("\"", "");
		String carrierInformation = carriersHashTable.get(carrierCode);
		if (carrierInformation != null) {
			try {
				Integer year = Integer.parseInt(flightInformation[0]);
				Integer miles = Integer.parseInt(flightInformation[18]);
				context.write(new Text(carrierInformation + "-" + year), new IntWritable(miles));
			}catch(NumberFormatException e) {
				//do nothing
				//either the year value is YEAR or the miles is NA
			}
		} else {
			System.out.println("Lookup for " + carrierCode + " gave null!");
		}
	}

	// we are using broadcast join because airports.csv is small enough to fit
	// into memory (about 280 KB)
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Path[] paths = DistributedCache.getArchiveClassPaths(context.getConfiguration());
		carriersHashTable = new HashMap<String, String>();
		FileSystem fs = null;
		FSDataInputStream inputStream = null;
		try {
			fs = FileSystem.get(context.getConfiguration());
			inputStream = fs
					.open(paths[0]);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// Read the broadcasted file
		BufferedReader br = new BufferedReader(new InputStreamReader(
				inputStream));
		// Hashtable to store the tuples

		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				String carrierInformation[] = line.split(",", 2);
				// Insert into Hashtable
				String carrierCoder = carrierInformation[0];
				carrierCoder = carrierCoder.replace("\"", "");
				String carrierName = carrierInformation[1].replace("\"", "");
				carriersHashTable.put(carrierCoder, carrierName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
