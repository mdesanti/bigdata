package ar.edu.itba.it.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormatSymbols;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TakeOffDelayMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private HashMap<String, String> airportsHashTable;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] flightInformation = value.toString().split(",");
		String IATA_code = flightInformation[16];
		String airportInformation = airportsHashTable.get(IATA_code);
		if (airportInformation != null) {
			String airportFields[] = airportInformation.split(",");
			String state = airportFields[2];
			String depDelay = flightInformation[15];
			Integer month = Integer.parseInt(flightInformation[1]);
			String monthString = new DateFormatSymbols().getMonths()[month-1];
			String sendToReducer = depDelay;
			String keyForReducer = state + "-" + monthString;
			context.write(new Text(keyForReducer), new Text(sendToReducer));
		} else {
			System.out.println("Lookup for " + IATA_code + " gave null!");
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		airportsHashTable = new HashMap<String, String>();
		FileSystem fs = null;
		FSDataInputStream inputStream = null;
		try {
			fs = FileSystem.get(context.getConfiguration());
			inputStream = fs
					.open(new Path("/user/mdesanti90/ref/airports.csv"));
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
				String airportInformation[] = line.split(",", 2);
				// Insert into Hashtable
				String IATA_code = airportInformation[0];
				IATA_code = IATA_code.replace("\"", "");
				airportsHashTable.put(IATA_code, airportInformation[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}