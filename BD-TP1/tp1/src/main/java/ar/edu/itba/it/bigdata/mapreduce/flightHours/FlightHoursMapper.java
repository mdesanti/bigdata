package ar.edu.itba.it.bigdata.mapreduce.flightHours;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ar.edu.itba.it.bigdata.mapreduce.Utils;

public class FlightHoursMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private HashMap<String, String> planeInformationHashTable = new HashMap<String, String>();
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
				context.write(new Text(tailNumber), new DoubleWritable(flightTime));
			} catch(NumberFormatException e) {
				//do nothing, flightTime can be NA
			}
		} else {
			//do nothing, plane might not be of the type we want!
		}
	}
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		this.planeType = context.getConfiguration().get("planeType");
		
		HTable table = Utils.getTable(context, "itba_tp1_planes");
		Scan scan = Utils.getScan("general");
		
		ResultScanner scanner = table.getScanner(scan);
		
		Iterator<Result> resultIterator = scanner.iterator();
		
		while(resultIterator.hasNext()) {
			Result result = resultIterator.next();
			
			List<KeyValue> list = result.getColumn("info".getBytes(), "manufacturer".getBytes());
			for(KeyValue kv: list) {
				String key = Bytes.toStringBinary ( kv.getKey(), 2, kv.getRowLength() );
				String manufacturer = new String(kv.getValue());
				if(manufacturer.equals(planeType)) {
					planeInformationHashTable.put(key, manufacturer);
				}
			}
		}
	}
}
