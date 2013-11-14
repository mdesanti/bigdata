package bolts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jdbc.ConnectionManager;
import jdbc.MySQLConnectionManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//Read: https://github.com/nathanmarz/storm/wiki/Tutorial
public class SystemOutBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	private ConnectionManager cm;
	private HashMap<String, List<String>> partiesKeywords = new HashMap<String, List<String>>();

	// public static Logger LOG = Logger.getLogger(SystemOutBolt.class);

	public SystemOutBolt(HashMap<String, List<String>> partiesKeywords) {
		this.cm = new MySQLConnectionManager();
		this.partiesKeywords = partiesKeywords;
	}

	@Override
	public void execute(Tuple tuple) {
		// Connection connection;
		// try {
		// connection = cm.getConnection();
		// PreparedStatement stmt = connection
		// .prepareStatement("insert into party(name ,quantity) values(?,?)");
		//
		// stmt.setString(1, "unen");
		// stmt.setInt(2, 1);
		// stmt.execute();
		//
		// connection.close();
		// } catch (SQLException e) {
		// e.printStackTrace();
		// }
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