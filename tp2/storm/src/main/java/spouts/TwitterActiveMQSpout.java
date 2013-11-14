package spouts;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterActiveMQSpout extends BaseRichSpout implements
		ExceptionListener {

	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(TwitterActiveMQSpout.class);
	private static String QUEUE_NAME = "TWITTER-G1";

	private MessageConsumer consumer;
	private Session session;
	private Connection connection;

	boolean _isDistributed;

	SpoutOutputCollector _collector;

	public TwitterActiveMQSpout() {
		this(true);
	}

	public TwitterActiveMQSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	private void connectToQueue() throws IOException {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				"tcp://10.117.39.161:61616");

		try {
			connection = connectionFactory.createConnection();
			connection.start();

			connection.setExceptionListener(this);

			// Create a Session
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// Create the destination (Topic or Queue)
			Destination destination = session.createQueue(QUEUE_NAME);

			// Create a MessageConsumer from the Session to the Topic or Queue
			consumer = session.createConsumer(destination);
		} catch (JMSException e) {
			e.printStackTrace();
		}

		// Configuration configuration = new Configuration();
		// FileSystem hdfs = FileSystem.get(configuration);
		// Path file = new Path("/ITBA/g1/logger.txt");
		// OutputStream os = hdfs.create(file);
		// BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os,
		// "UTF-8" ) );
		// br.write("Connected");
		// br.close();
		// hdfs.close();
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {
			connectToQueue();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		Message message;
		try {
			message = consumer.receive(1000);
			if (message instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) message;
				String text = textMessage.getText();
				_collector.emit(new Values(text));
			} else {
			}
		} catch (JMSException e) {
			e.printStackTrace();
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
