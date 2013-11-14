package jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Provides a JDBC connection to a PostgreSQL server.
 * 
 */

public class MySQLConnectionManager implements ConnectionManager {
	private boolean inUse = false;
	private Connection connection;

	private String dbName;
	private String dbPort;
	private String dbHost;
	private String username;
	private String password;

	public MySQLConnectionManager() {
		super();

	}

	public Connection getConnection() throws SQLException {
		Properties myProps = new Properties();
		InputStream MyInputStream = null;
		MyInputStream = this.getClass().getClassLoader()
				.getResourceAsStream("db.properties");
		try {
			myProps.load(MyInputStream);
		} catch (IOException e1) {
			throw new SQLException();
		}

		dbName = myProps.getProperty("dbName");
		dbPort = myProps.getProperty("dbPort");
		dbHost = myProps.getProperty("dbHost");
		username = myProps.getProperty("username");
		password = myProps.getProperty("password");

		try {
			MyInputStream.close();
		} catch (IOException e1) {
			throw new SQLException();
		}
		try {
			this.connection = DriverManager.getConnection("jdbc:mysql://"
					+ dbHost + ":" + dbPort + "/" + dbName, username, password);
		} catch (SQLException e) {
			throw new SQLException();
		}

		return connection;
	}

	public ConnectionManager release() {
		if (!this.inUse) {
			throw new IllegalStateException(
					"Cannot release connection that is not in use.");
		}
		this.inUse = false;
		return this;
	}

	public void close() throws SQLException {
		try {
			this.connection.close();
		} catch (SQLException e) {
			throw new SQLException();
		}
	}

	public Connection hold() {
		if (this.inUse) {
			throw new IllegalStateException(
					"Cannot hold connection that is in use.");
		}
		this.inUse = true;
		return this.connection;
	}
}
