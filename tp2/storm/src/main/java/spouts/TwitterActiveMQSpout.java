package spouts;

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

public class TwitterActiveMQSpout extends BaseRichSpout implements ExceptionListener {
	
	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(TwitterActiveMQSpout.class);
	
	private MessageConsumer consumer;
	private Session session;
	private Connection connection;
	
	boolean _isDistributed;
	
	SpoutOutputCollector _collector;

	public TwitterActiveMQSpout() {
		this(true);
		connectToQueue();
	}

	public TwitterActiveMQSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
		connectToQueue();
	}

	private void connectToQueue() {
		//aca que ponemos?
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
		 
		try {
			connection = connectionFactory.createConnection();
			connection.start();

	        connection.setExceptionListener(this);

	        // Create a Session
	        session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

	        // Create the destination (Topic or Queue)
	        Destination destination = session.createQueue("TWITTER");

	        // Create a MessageConsumer from the Session to the Topic or Queue
	        consumer = session.createConsumer(destination);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void close() {
		try {
			consumer.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void nextTuple() {
		Message message;
		try {
			message = consumer.receive(1000);
			if (message instanceof TextMessage) {
	            TextMessage textMessage = (TextMessage) message;
	            String text = textMessage.getText();
	            System.out.println("Received: " + text);
	            _collector.emit(new Values(text));
	        } else {
	            System.out.println("Received: " + message);
	        }
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

	public void onException(JMSException arg0) {
		//log error
	}

}
