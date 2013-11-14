package topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	private static HTable getTable(String tableName) {
		Configuration hConf = HBaseConfiguration.create();
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				"hadoop-2013-datanode-1");
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
		try {
			return new HTable(hConf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static HashMap<String, List<String>> partiesReader()
			throws IOException, InterruptedException {
		HashMap<String, List<String>> partiesKeywords = new HashMap<String, List<String>>();
		HTable table = getTable("itba_tp2_twitter_words");
		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(scan);

		Iterator<Result> resultIterator = scanner.iterator();
		while (resultIterator.hasNext()) {
			Result result = resultIterator.next();
			List<String> aux = new ArrayList<String>();
			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes
					.toBytes("keyword"));
			for (byte[] qualifier : familyMap.keySet()) {
				byte[] q = familyMap.get(qualifier);
				aux.add(Bytes.toString(q));
			}
			partiesKeywords.put(Bytes.toString(result.getRow()), aux);
		}
		return partiesKeywords;
	}

}
