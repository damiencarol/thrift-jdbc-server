package driver;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;

import driver.io.CCConnection;
import driver.io.CCSQLException;
import driver.io.CCStatement;
import driver.io.ConnectionService.Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrystalConnection implements Connection {

    final Logger logger = LoggerFactory.getLogger(CrystalConnection.class);

    CCConnection connection;
    private Client client;
    private TSocket transport;
    private boolean isClosed;

    public CrystalConnection(TSocket transport, Client client,
            CCConnection conn, String url, Properties info) {
        this.transport = transport;
        this.client = client;
        connection = conn;

        this.isClosed = false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        try {
            this.client.connection_setAutoCommit(connection, autoCommit);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public boolean getAutoCommit() throws SQLException {
        try {
            return this.client.connection_getAutoCommit(connection);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void commit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void rollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void close() throws SQLException {
        if (!this.isClosed)
            try {
                this.client.closeConnection(connection);

                this.connection = null;
                this.client = null;

            } catch (CCSQLException ex) {
                throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode,
                        ex);
            } catch (TException e) {
                throw new SQLException(
                        "Error while cleaning up the server resources", e);
            } finally {
                this.isClosed = true;
                if (this.transport != null)
                    this.transport.close();
            }

    }

    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new CrystalDatabaseMetaData(this.client, this);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            this.client.connection_setReadOnly(connection, readOnly);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public boolean isReadOnly() throws SQLException {
        try {
            return this.client.connection_getReadOnly(connection);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void setCatalog(String catalog) throws SQLException {
        try {
            this.client.connection_setCatalog(connection, catalog);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public String getCatalog() throws SQLException {
        try {
            return this.client.connection_getCatalog(connection);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        try {
            this.client.connection_setTransactionIsolation(connection, level);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public int getTransactionIsolation() throws SQLException {
        try {
            return this.client.connection_getTransactionIsolation(connection);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public int getHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            CCStatement stat = this.client.createStatement(connection);
            return new CrystalStatement(client, this, stat);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Method not supported: prepareStatement");
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Clob createClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean isValid(int timeout) throws SQLException {
        try {
            return this.client.connection_isvalid(connection, timeout);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setSchema(String schema) throws SQLException {
        try {
            this.client.connection_setSchema(connection, schema);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public String getSchema() throws SQLException {
        try {
            return this.client.connection_getSchema(connection);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("Method not supported");
    }

}
