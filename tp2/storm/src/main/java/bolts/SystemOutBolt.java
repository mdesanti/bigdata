package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import jdbc.ConnectionManager;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

//Read: https://github.com/nathanmarz/storm/wiki/Tutorial
public class SystemOutBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    public static Logger LOG = Logger.getLogger(SystemOutBolt.class);
    private static Charset CHARSET = Charset.forName("ISO-8859-1");
    OutputCollector _collector;
    private ConnectionManager cm;
    private HashMap<String, String> partiesKeywords = new HashMap<String, String>();

    public SystemOutBolt(HashMap<String, String> partiesKeywords, ConnectionManager manager) {
        this.cm = manager;
        this.partiesKeywords = partiesKeywords;
    }

    @Override
    public void execute(Tuple tuple) {

		String text = tuple.getString(0);
        if (text != null && text.length() != 0) {
            Connection connection = null;
            try {
                connection = cm.getConnection();
                String[] words = text.split(" ");
                for (String word : words) {
                    word = word.replace("#", "").toLowerCase();
                    String party = partiesKeywords.get(word);
                    if (party != null) {
                        PreparedStatement stmt = connection
                                .prepareStatement("insert into party(name, quantity) values(?,1) ON DUPLICATE KEY UPDATE quantity = quantity + 1;");
                        stmt.setString(1, party);
                        stmt.execute();
                    }
                }
            } catch (SQLException e) {
                LOG.log(Level.ERROR, "SQL error in Bolt \n" + ExceptionUtils.getStackTrace(e));
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                    }
                }
            }
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