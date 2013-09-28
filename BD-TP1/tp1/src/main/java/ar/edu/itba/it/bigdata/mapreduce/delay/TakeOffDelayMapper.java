package ar.edu.itba.it.bigdata.mapreduce.delay;

import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ar.edu.itba.it.bigdata.mapreduce.Utils;

public class TakeOffDelayMapper extends Mapper<LongWritable, Text, Text, Text> {

	private HashMap<String, String> airportsHashTable = new HashMap<String, String>();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] flightInformation = value.toString().split(",");
		String IATA_code = flightInformation[16];
		String state = airportsHashTable.get(IATA_code);
		if (state != null) {
			String depDelay = flightInformation[15];
			String month = getMonth(flightInformation[1]);
			String sendToReducer = depDelay;
			String keyForReducer = state + "-" + month;
			context.write(new Text(keyForReducer), new Text(sendToReducer));
		} else {
			System.out.println("Lookup for " + IATA_code + " gave null!");
		}
	}

	private String getMonth(String monthNumber) {
		Integer month = Integer.parseInt(monthNumber);
		return new DateFormatSymbols().getMonths()[month - 1];
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		HTable table = Utils.getTable(context, "itba_tp1_airports");
		Scan scan = Utils.getScan("info");
		
		ResultScanner scanner = table.getScanner(scan);
		
		Iterator<Result> resultIterator = scanner.iterator();
		
		while(resultIterator.hasNext()) {
			Result result = resultIterator.next();
			
			List<KeyValue> list = result.getColumn("info".getBytes(), "state".getBytes());
			for(KeyValue kv: list) {
				String key = Bytes.toStringBinary ( kv.getKey(), 2, kv.getRowLength() );
				String value = new String(kv.getValue());
				airportsHashTable.put(key, value);
			}
		}
	}
}
