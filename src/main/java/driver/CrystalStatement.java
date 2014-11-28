package driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.thrift.TException;

import driver.io.CCResultSet;
import driver.io.CCSQLException;
import driver.io.CCStatement;
import driver.io.ConnectionService.Client;
import driver.io.statement_getWarnings_return;

public class CrystalStatement implements Statement {

    private Client client;
    private CCStatement stat;
    
    private CrystalConnection connection;


    public CrystalStatement(Client client, CrystalConnection connection, CCStatement stat) {
        this.client = client;
        this.connection = connection;
        this.stat = stat;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported: unwrap");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported: isWrapperFor");
        //return iface == java.sql.Statement.class || iface == CrystalStatement.class;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            CCResultSet resultset = this.client.statement_executeQuery(stat, sql);
            return new CrystalResultSet(resultset);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }

    public void close() throws SQLException {
        try {
            this.client.statement_close(this.stat);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Method not supported: getMaxFieldSize");
    }

    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Method not supported: setMaxFieldSize");
    }

    public int getMaxRows() throws SQLException {
        try {
            return this.client.statement_getMaxRows(this.stat);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void setMaxRows(int max) throws SQLException {
        try {
            this.client.statement_setMaxRows(this.stat, max);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("Method not supported: setEscapeProcessing");
    }

    public int getQueryTimeout() throws SQLException {
        throw new SQLException("Method not supported: getQueryTimeout");
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLException("Method not supported: setQueryTimeout");
    }

    public void cancel() throws SQLException {
        try {
            this.client.statement_cancel(this.stat);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        try {
            statement_getWarnings_return warn = this.client.statement_getWarnings(this.stat);
            return CrystalWarning.buildFromList(warn.warnings);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void clearWarnings() throws SQLException {
        try {
            this.client.statement_clearWarnings(this.stat);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Method not supported: setCursorName");
    }

    public boolean execute(String sql) throws SQLException {
        try {
            return this.client.statement_execute(this.stat, sql);
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet() throws SQLException {
        try {
            return new CrystalResultSet(this.client.statement_getResultSet(this.stat));
        } catch (CCSQLException ex) {
            throw new SQLException(ex.reason, ex.sqlState, ex.vendorCode, ex);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public int getUpdateCount() throws SQLException {
        try {
            return this.client.statement_getUpdateCount(this.stat);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Method not supported: getMoreResults");
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Method not supported: setFetchDirection");
    }

    public int getFetchDirection() throws SQLException {
        throw new SQLException("Method not supported: getFetchDirection");
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("Method not supported: setFetchSize");
    }

    public int getFetchSize() throws SQLException {
        throw new SQLException("Method not supported: getFetchSize");
    }

    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Method not supported: getResultSetConcurrency");
    }

    public int getResultSetType() throws SQLException {
        try {
            return this.client.statement_getResultSetType(this.stat);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public void addBatch(String sql) throws SQLException {
        throw new SQLException("Method not supported: addBatch");
    }

    public void clearBatch() throws SQLException {
        throw new SQLException("Method not supported: clearBatch");
    }

    public int[] executeBatch() throws SQLException {
        throw new SQLException("Method not supported: executeBatch");
    }

    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("Method not supported: getMoreResults");
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Method not supported: getGeneratedKeys");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported:executeUpdate");
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported: executeUpdate");
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported: execute");
    }

    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Method not supported: getResultSetHoldability");
    }

    public boolean isClosed() throws SQLException {
        throw new SQLException("Method not supported: isClosed");
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException("Method not supported: setPoolable");
    }

    public boolean isPoolable() throws SQLException {
        throw new SQLException("Method not supported: isPoolable");
    }

    public void closeOnCompletion() throws SQLException {
        throw new SQLException("Method not supported: closeOnCompletion");
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLException("Method not supported: isCloseOnCompletion");
    }

}
