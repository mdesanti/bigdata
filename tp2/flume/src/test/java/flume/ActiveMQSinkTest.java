package flume;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flume.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.core.classloader.annotations.PrepareForTest;

import javax.jms.*;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Created by cristian@redmintlabs.com on 11/16/13 at 15:55
 */
@PrepareForTest(ActiveMQSink.class)
public class ActiveMQSinkTest {

    private ActiveMQSink sink;

    @Before
    public void init() {
        sink = new ActiveMQSink();
    }

    @Test
    public void testInitConnection() throws JMSException {
        sink = spy(sink);

        ActiveMQConnectionFactory mockFactory = mock(ActiveMQConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        when(sink.getConnectionFactory()).thenReturn(mockFactory);
        when(mockFactory.createConnection()).thenReturn(mockConnection);

        assertNotNull(sink.initConnection());

        verify(mockConnection).start();
    }

    @Test
    public void testGetConnectionFactory() throws Exception {
        sink = spy(sink);

        ActiveMQConnectionFactory mockFactory = mock(ActiveMQConnectionFactory.class);
        whenNew(ActiveMQConnectionFactory.class).withAnyArguments().thenReturn(mockFactory);
        ActiveMQConnectionFactory factory = sink.getConnectionFactory();

        assertNotNull(factory);
    }

    @Test
    public void testStart() throws JMSException {
        sink = spy(sink);

        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);

        doReturn(mockConnection).when(sink).initConnection();
        when(mockConnection.createSession(anyBoolean(), anyInt()))
                .thenReturn(mockSession);
        when(mockSession.createProducer(Matchers.<Destination>any()))
                .thenReturn(mock(MessageProducer.class));

        sink.start();

        // Creates a session
        verify(mockConnection).createSession(anyBoolean(), anyInt());

        // Creates a producer
        verify(mockSession).createProducer(Matchers.<Destination>any());
    }

    @Test
    public void testStop() throws JMSException {
        sink.session = mock(Session.class);
        sink.connection = mock(Connection.class);
        sink.producer = mock(MessageProducer.class);

        sink.stop();

        verify(sink.session).close();
        verify(sink.connection).close();
        verify(sink.producer).close();
    }


    @Test
    public void testProcessHappyPath() throws EventDeliveryException, JMSException {
        sink = spy(sink);
        sink.session = mock(Session.class);
        sink.producer = mock(MessageProducer.class);

        Channel mockChannel = mock(Channel.class);
        Transaction mockTransaction = mock(Transaction.class);
        Event mockEvent = mock(Event.class);
        TextMessage aMessage = mock(TextMessage.class);

        doReturn(mockChannel).when(sink).getChannel();
        doReturn(mockEvent).when(mockChannel).take();
        doReturn(mockTransaction).when(mockChannel).getTransaction();
        doReturn(new byte[] {}).when(mockEvent).getBody();
        doReturn(aMessage).when(sink.session).createTextMessage(anyString());

        Sink.Status status = sink.process();

        assertEquals(status, Sink.Status.READY);

        verify(mockTransaction).begin();
        verify(sink.producer).send(aMessage);
        verify(mockTransaction).commit();
        verify(mockTransaction).close();
        verifyNoMoreInteractions(mockTransaction);
    }

    @Test
    public void testProcessExceptionPath() throws EventDeliveryException, JMSException {
        sink = spy(sink);
        sink.session = mock(Session.class);
        sink.producer = mock(MessageProducer.class);

        Channel mockChannel = mock(Channel.class);
        Transaction mockTransaction = mock(Transaction.class);
        Event mockEvent = mock(Event.class);
        TextMessage aMessage = mock(TextMessage.class);

        doReturn(mockChannel).when(sink).getChannel();
        doThrow(Exception.class).when(mockChannel).take();
        doReturn(mockTransaction).when(mockChannel).getTransaction();

        Sink.Status status = sink.process();

        assertEquals(status, Sink.Status.BACKOFF);

        verify(mockTransaction).begin();
        verify(mockTransaction).rollback();
        verify(mockTransaction).close();
        verifyNoMoreInteractions(mockTransaction);
    }

}
