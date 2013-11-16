package bolts;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import jdbc.ConnectionManager;
import jdbc.MySQLConnectionManager;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
	private HashMap<String, String> partiesKeywords = new HashMap<String, String>();

	public static Logger LOG = Logger.getLogger(SystemOutBolt.class);

	public SystemOutBolt(HashMap<String, String> partiesKeywords) {
		this.cm = new MySQLConnectionManager();
		this.partiesKeywords = partiesKeywords;
	}

	@Override
	public void execute(Tuple tuple) {

		JSONObject json = null;
		try {
			json = (JSONObject) new JSONParser().parse(tuple.getString(0));
			String text = new String(((String) json.get("text")).getBytes(),
					CHARSET).toLowerCase();
			LOG.log(Level.INFO, "GOT TEXT => " + text);
			Connection connection;
			try {
				connection = cm.getConnection();
				String[] words = text.split(" ");
				for (String word : words) {
					LOG.debug("Found hit for keyword: " + word
							+ " in " + text);
					word = word.replace("#", "").toLowerCase();
					String party = partiesKeywords.get(word);
					if (party != null) {
						PreparedStatement stmt = connection
								.prepareStatement("insert into party(name, quantity) values(?,1) ON DUPLICATE KEY UPDATE quantity = quantity + 1;");
						stmt.setString(1, party);
						stmt.execute();
					}
				}
				connection.close();
			} catch (SQLException e) {
				LOG.log(Level.ERROR, "SQL error in Bolt \n" + ExceptionUtils.getStackTrace(e));
			}
		} catch (ParseException e1) {
			LOG.log(Level.ERROR, "Parse error in Bolt \n" + ExceptionUtils.getStackTrace(e1));
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