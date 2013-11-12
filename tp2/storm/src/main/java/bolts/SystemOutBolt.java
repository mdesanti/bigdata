package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SystemOutBolt extends BaseRichBolt {

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	public static Logger LOG = Logger.getLogger(SystemOutBolt.class);

	public void execute(Tuple tuple) {
		_collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("word"));
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}