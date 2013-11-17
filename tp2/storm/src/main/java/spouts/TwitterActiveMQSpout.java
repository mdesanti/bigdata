package spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.jms.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class TwitterActiveMQSpout extends BaseRichSpout implements
		ExceptionListener {

	private static final long serialVersionUID = 1L;
	private static Charset CHARSET = Charset.forName("ISO-8859-1");
	public static Logger LOG = Logger.getLogger(TwitterActiveMQSpout.class);
	private static String QUEUE_NAME = "TWITTER-G1";

	MessageConsumer consumer;
	Session session;
	Connection connection;

	boolean _isDistributed;

	SpoutOutputCollector _collector;
    ActiveMQConnectionFactory connectionFactory;

    public TwitterActiveMQSpout() {
		this(true);
	}

	public TwitterActiveMQSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

    void connectToQueue() {
        try {
            connection = initConnection();
            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(QUEUE_NAME);

            // Create a MessageProducer from the Session to the Queue
            consumer = session.createConsumer(destination);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public Connection initConnection() throws JMSException {
        connection = getConnectionFactory().createConnection();
        connection.start();
        return connection;
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://10.117.39.161:61616");
        }
        return connectionFactory;
    }


	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
        connectToQueue();
	}

	@Override
	public void close() {
		try {
			consumer.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		Message message = null;
		JSONObject json = null;
		try {
			message = consumer.receive(1000);
			if (message instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) message;
				String text = textMessage.getText();
				json = (JSONObject) new JSONParser().parse(text);
				String tweet = new String(((String) json.get("text")).getBytes(),
						CHARSET).toLowerCase();
				_collector.emit(new Values(tweet));
			} else {
			}
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			TextMessage textMessage = (TextMessage) message;
			String text;
			try {
				text = textMessage.getText();
				LOG.log(Level.ERROR, "Parse error in Spout \n" + ExceptionUtils.getStackTrace(e) + "\n For Text: " + text);
			} catch (JMSException e1) {
				LOG.log(Level.ERROR, ExceptionUtils.getStackTrace(e));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public void onException(JMSException arg0) {

	}

}
