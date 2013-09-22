package ar.edu.itba.it.bigdata.mapreduce.flightHours;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormatSymbols;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightHoursMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private HashMap<String, String> planeInformationHashTable;
	private String planeType;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] flightInformation = value.toString().split(",");
		String tailNumber = flightInformation[10];
		String planeType = planeInformationHashTable.get(tailNumber);
		if (planeType != null) {
			String airTime = flightInformation[13];
			try {
				Double flightTime = Double.parseDouble(airTime);
				context.write(new Text(planeType), new DoubleWritable(flightTime));
			} catch(NumberFormatException e) {
				//do nothing, flightTime can be NA
			}
		} else {
			
		}
	}
	// we are using broadcast join because airports.csv is small enough to fit
	// into memory (about 280 KB)
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.planeType = context.getConfiguration().get("planeType");
		planeInformationHashTable = new HashMap<String, String>();
		FileSystem fs = null;
		FSDataInputStream inputStream = null;
		try {
			fs = FileSystem.get(context.getConfiguration());
			inputStream = fs
					.open(new Path("/user/mdesanti90/ref/plane-data.csv"));
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
				String planeInformation[] = line.split(",");
				if(planeInformation.length > 1) {
					// Insert into Hashtable
					String tailNumber = planeInformation[0];
					String planeType = planeInformation[2];
					tailNumber = tailNumber.replace("\"", "");
					planeType = planeType.replace("\"", "");
					if(planeType.equals(this.planeType)) {
						planeInformationHashTable.put(tailNumber, planeType);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
