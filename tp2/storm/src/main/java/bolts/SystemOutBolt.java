package bolts;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jdbc.ConnectionManager;
import jdbc.MySQLConnectionManager;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

//Read: https://github.com/nathanmarz/storm/wiki/Tutorial
public class SystemOutBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static Charset CHARSET = Charset.forName("ISO-8859-1");
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
		
		JSONObject json = null;
		try {
			json = (JSONObject) new JSONParser().parse(tuple.getString(0));
			String text = new String(((String) json.get("text")).getBytes(), CHARSET);
			for (String party : partiesKeywords.keySet()) {
				for (String keyword : partiesKeywords.get(party)) {
					if(text.contains(keyword)) {
						Connection connection;
						try {
							connection = cm.getConnection();
							PreparedStatement stmt = connection
									.prepareStatement("insert into party(name ,quantity) values(?,?)");
							
							stmt.setString(1, keyword);
							stmt.setInt(2, 1);
							stmt.execute();
							
							connection.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						
					}
				}
			}
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
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