package jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides a JDBC connection to a database.
 * 
 */
public interface ConnectionManager {

	/**
	 * Returns a connection.
	 * 
	 * @return Connection
	 * @throws MySQLException
	 */
	public Connection getConnection() throws SQLException;

	/**
	 * Releases a connection.
	 * 
	 * @return ConnectionManager
	 * @throws IllegalStateException
	 *             If trying to release a connection that is not in use.
	 */
	public ConnectionManager release();

	/**
	 * Holds a connection
	 * 
	 * @return
	 */
	public Connection hold();

	/**
	 * Closes a connection.
	 * 
	 * @throws SQLException
	 */
	public void close() throws SQLException;

}
