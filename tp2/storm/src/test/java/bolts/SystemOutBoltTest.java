package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import jdbc.ConnectionManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Created by cristian@redmintlabs.com on 11/16/13 at 22:57
 */
public class SystemOutBoltTest {

    SystemOutBolt bolt;

    ConnectionManager       mockManager;
    Connection              mockConnection;
    HashMap<String, String> partyKeywords;
    PreparedStatement mockStatement;
    OutputCollector mockCollector;

    @Before
    public void init() throws SQLException {
        partyKeywords  = new HashMap<String, String>();
        mockManager    = mock(ConnectionManager.class);
        mockConnection = mock(Connection.class);
        mockStatement  = mock(PreparedStatement.class);
        mockCollector  = mock(OutputCollector.class);

        when(mockManager.getConnection()).thenReturn(mockConnection);

        partyKeywords.put("a", "b");
        partyKeywords.put("c", "d");

        bolt            = new SystemOutBolt(partyKeywords, mockManager);
        bolt._collector = mockCollector;
    }

    @Test
    public void testHappyPathExecute() throws SQLException {
        Tuple mockTuple = mock(Tuple.class);

        when(mockConnection.prepareStatement(Matchers.<String>any()))
                .thenReturn(mockStatement);
        when(mockTuple.getString(0)).thenReturn("a");

        bolt.execute(mockTuple);

        verify(mockStatement).setString(1, "b");
        verify(mockStatement).execute();
        verifyNoMoreInteractions(mockStatement);
        verify(mockCollector).ack(mockTuple);
        verify(mockConnection).close();
    }

    @Test
    public void testHappyPathMultipleMatchesExecute() throws SQLException {
        Tuple mockTuple = mock(Tuple.class);

        when(mockConnection.prepareStatement(Matchers.<String>any()))
                .thenReturn(mockStatement);
        when(mockTuple.getString(0)).thenReturn("a c a");

        bolt.execute(mockTuple);

        verify(mockStatement, times(2)).setString(1, "b");
        verify(mockStatement).setString(1, "d");
        verify(mockStatement, times(3)).execute();
        verifyNoMoreInteractions(mockStatement);
        verify(mockCollector).ack(mockTuple);
    }

    @Test
    public void testNoMatchesOnExecute() throws SQLException {
        Tuple mockTuple = mock(Tuple.class);

        when(mockConnection.prepareStatement(Matchers.<String>any()))
                .thenReturn(mockStatement);
        when(mockTuple.getString(0)).thenReturn("zarasa");

        bolt.execute(mockTuple);

        verifyNoMoreInteractions(mockStatement);
        verify(mockCollector).ack(mockTuple);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNoConnectionClosesDatabaseOnExecute() throws SQLException {
        Tuple mockTuple = mock(Tuple.class);

        when(mockConnection.prepareStatement(Matchers.<String>any()))
                .thenThrow(SQLException.class);
        when(mockTuple.getString(0)).thenReturn("zarasa");

        bolt.execute(mockTuple);

        verify(mockConnection).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNoConnectionIsStablishedWhenEmptyOrNullTextOnExecute() throws SQLException {
        Tuple mockTuple = mock(Tuple.class);

        bolt.execute(mockTuple);

        verifyZeroInteractions(mockManager);
    }
}
