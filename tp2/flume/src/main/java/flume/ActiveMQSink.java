package flume;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import javax.jms.*;

public class ActiveMQSink extends AbstractSink implements Configurable {

	private static String TOPIC_NAME = "TWITTER";

	private String myProp;

	private MessageProducer producer;
	private Session session;
	private Connection connection;

	public void configure(Context context) {
		String myProp = context.getString("myProp", "defaultValue");

		// Process the myProp value (e.g. validation)

		// Store myProp for later retrieval by process() method
		this.myProp = myProp;
	}

	public void start() {
		// Create a ConnectionFactory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				"tcp://10.117.39.161:61616");

		try {
			connection = connectionFactory.createConnection();
			connection.start();

			// Create a Session
			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

			// Create the destination (Topic or Queue)
			Destination destination = session.createQueue(TOPIC_NAME);

			// Create a MessageProducer from the Session to the Topic or Queue
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		// Clean up
		try {
			session.close();
			connection.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Status process() throws EventDeliveryException {
		Status status = null;

		System.out.println("------------- GOT TWEET !");
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			// This try clause includes whatever Channel operations you want to
			// do

			Event event = ch.take();
			if (event != null) {
				String text = new String(event.getBody());
				System.out.println("Something's in the sink! " + text);

				// Create a messages
				TextMessage message = session.createTextMessage(text);

				// Tell the producer to send the message
				producer.send(message);

				txn.commit();
				status = Status.READY;
			}
		} catch (Throwable t) {
			System.out.println("------------- GOT EXCEPTION! ");
			txn.rollback();

			// Log exception, handle individual exceptions as needed

			status = Status.BACKOFF;

			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

}
