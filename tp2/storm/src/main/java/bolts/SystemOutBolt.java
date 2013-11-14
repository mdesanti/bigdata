package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


//Read: https://github.com/nathanmarz/storm/wiki/Tutorial
public class SystemOutBolt extends BaseRichBolt {

	// public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM =
	// "hbase.zookeeper.quorum";
	// public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT =
	// "hbase.zookeeper.property.clientPort";

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	// public static Logger LOG = Logger.getLogger(SystemOutBolt.class);

	@Override
	public void execute(Tuple tuple) {
		_collector.emit(tuple, new Values("pepe execute"));
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("word"));
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

}