package topology;

import spouts.TwitterActiveMQSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolts.SystemOutBolt;

/**
 * This is a basic example of a Storm topology.
 */
public class Topology {
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new TwitterActiveMQSpout());        
        builder.setBolt("counter", new SystemOutBolt());
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}