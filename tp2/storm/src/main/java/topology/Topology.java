package topology;

import jdbc.MySQLConnectionManager;
import spouts.TwitterActiveMQSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolts.SystemOutBolt;

public class Topology {


	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets", new TwitterActiveMQSpout(), 3);
		builder.setBolt("printer",
				new SystemOutBolt(HBaseUtil.partiesReader(), new MySQLConnectionManager()), 10)
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
}