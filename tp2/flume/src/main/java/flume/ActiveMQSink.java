package flume;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import javax.jms.*;

public class ActiveMQSink extends AbstractSink implements Configurable {

	private static String TOPIC_NAME = "TWITTER-G1";
	private static String ACTIVEMQ_HOST = "tcp://54.234.240.27:61616";

	MessageProducer producer;
	Session session;
	Connection connection;
    ActiveMQConnectionFactory connectionFactory;

	@Override
	public void configure(Context context) {
	}

	@Override
	public void start() {
		try {
            connection = initConnection();
			// Create a Session
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// Create the destination (Topic or Queue)
			Destination destination = session.createQueue(TOPIC_NAME);

			// Create a MessageProducer from the Session to the Queue
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

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
                ACTIVEMQ_HOST);
        }
        return connectionFactory;
    }

	@Override
	public void stop() {
		try {
			session.close();
			connection.close();
            producer.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;

		Channel ch = getChannel();
		Transaction txn = getChannel().getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			String text = new String(event.getBody());
			
			// Create a messages
			TextMessage message = session.createTextMessage(text);
			// Tell the producer to send the message
			producer.send(message);

			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;
		} finally {
			txn.close();
		}
		return status;
	}
}
