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

import spouts.TwitterActiveMQSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolts.SystemOutBolt;

public class Topology {

	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets", new TwitterActiveMQSpout(), 3);
		builder.setBolt("printer", new SystemOutBolt(partiesReader()), 10)
				.shuffleGrouping("tweets");

		Config conf = new Config();

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormTopology topology = builder.createTopology();
			StormSubmitter.submitTopology(args[0], conf, topology);
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

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

	private static Scan getScan(String family) {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.addFamily(family.getBytes());
		return scan;
	}

	private static HashMap<String, List<String>> partiesReader()
			throws IOException, InterruptedException {

		HashMap<String, List<String>> partiesKeywords = new HashMap<String, List<String>>();
		HTable table = getTable("itba_tp2_twitter_words");
		Scan scan = getScan("keyword");

		ResultScanner scanner = table.getScanner(scan);

		Iterator<Result> resultIterator = scanner.iterator();

		while (resultIterator.hasNext()) {
			Result result = resultIterator.next();
			List<String> aux = new ArrayList<String>();

			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes
					.toBytes("keyword"));
			for (byte[] qualifier : familyMap.keySet()) {
				aux.add(Bytes.toString(qualifier));
			}
			partiesKeywords.put(result.getRow().toString(), aux);

		}
		return partiesKeywords;
	}
}