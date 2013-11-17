package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.core.classloader.annotations.PrepareForTest;

import javax.jms.*;
import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.*;


/**
 * Created by cristian@redmintlabs.com on 11/16/13 at 17:50
 */
@PrepareForTest(TwitterActiveMQSpout.class)
public class TwitterActiveMQSpoutTest {

    private TwitterActiveMQSpout spout;

    @Before
    public void init() {
        spout = new TwitterActiveMQSpout();
    }


    @Test
    public void testInitConnection() throws JMSException {
        spout = spy(spout);

        ActiveMQConnectionFactory mockFactory = mock(ActiveMQConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        when(spout.getConnectionFactory()).thenReturn(mockFactory);
        when(mockFactory.createConnection()).thenReturn(mockConnection);

        assertNotNull(spout.initConnection());

        verify(mockConnection).start();
    }

    @Test
    public void testGetConnectionFactory() throws Exception {
        spout = spy(spout);

        ActiveMQConnectionFactory mockFactory = mock(ActiveMQConnectionFactory.class);
        whenNew(ActiveMQConnectionFactory.class).withAnyArguments().thenReturn(mockFactory);
        ActiveMQConnectionFactory factory = spout.getConnectionFactory();

        assertNotNull(factory);
    }

    @Test
    public void testStart() throws JMSException {
        spout = spy(spout);

        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);

        doReturn(mockConnection).when(spout).initConnection();
        when(mockConnection.createSession(anyBoolean(), anyInt()))
                .thenReturn(mockSession);
        when(mockSession.createProducer(Matchers.<Destination>any()))
                .thenReturn(mock(MessageProducer.class));

        spout.connectToQueue();

        verify(mockConnection).createSession(anyBoolean(), anyInt());
        verify(mockSession).createConsumer(Matchers.<Destination>any());
    }

    @Test
    public void testClose() throws JMSException {
        spout.session = mock(Session.class);
        spout.connection = mock(Connection.class);
        spout.consumer = mock(MessageConsumer.class);

        spout.close();

        verify(spout.session).close();
        verify(spout.connection).close();
        verify(spout.consumer).close();
    }

    @Test
    public void testHappyPathNextTuple() throws JMSException {
        spout.consumer = mock(MessageConsumer.class);
        spout._collector = mock(SpoutOutputCollector.class);

        TextMessage mockMessage = mock(TextMessage.class);

        when(spout.consumer.receive(anyInt())).thenReturn(mockMessage);
        when(mockMessage.getText()).thenReturn("{ \"text\": \"testTweet\" }");
        doReturn(null).when(spout._collector).emit(Matchers.<List<Object>>anyObject());

        spout.nextTuple();

        verify(spout._collector).emit(Matchers.<List<Object>>anyObject());
    }

    @Test
    public void testUnhappyPathNextTuple() throws JMSException {
        spout.consumer = mock(MessageConsumer.class);
        spout._collector = mock(SpoutOutputCollector.class);

        when(spout.consumer.receive(anyInt())).thenReturn(null);

        spout.nextTuple();

        verifyNoMoreInteractions(spout._collector);
    }
}
