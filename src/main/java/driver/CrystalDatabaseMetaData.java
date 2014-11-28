package driver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import driver.io.CCConnection;
import driver.io.CCResultSet;
import driver.io.CCSQLException;
import driver.io.ConnectionService.Client;

public class CrystalDatabaseMetaData implements DatabaseMetaData {
    
    final Logger logger = LoggerFactory.getLogger(CrystalDatabaseMetaData.class);

    private CrystalConnection crystalConnection;
    private CCConnection connection;

    public CrystalDatabaseMetaData(CrystalConnection connection) {
        this.crystalConnection = connection;
        this.connection = connection.connection;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean deletesAreDetected(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getAttributes(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String arg0, String arg1,
            String arg2, int arg3, boolean arg4) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getCatalogSeparator(connection);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getCatalogTerm(connection);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resultset = client.connection_getCatalogs(connection);
            return new CrystalResultSet(resultset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resulset = client.connection_getColumns(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
            return new CrystalResultSet(resulset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.crystalConnection;
    }

    @Override
    public ResultSet getCrossReference(String arg0, String arg1, String arg2,
            String arg3, String arg4, String arg5) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getDatabaseMajorVersion();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getDatabaseMinorVersion();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getDatabaseProductName();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getDatabaseProductVersion();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getDefaultTransactionIsolation();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public int getDriverMajorVersion() {
        return 4;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Crystal";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "0.1";
    }

    @Override
    public ResultSet getExportedKeys(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getFunctions(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).getIdentifierQuoteString();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getImportedKeys(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getIndexInfo(String arg0, String arg1, String arg2,
            boolean arg3, boolean arg4) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 1;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getProcedureColumns(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getProcedures(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getPseudoColumns(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getSQLKeywords(connection);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public int getSQLStateType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getSchemaTerm(connection);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resultset = client.connection_getSchemas(connection, catalog, schemaPattern);
            return new CrystalResultSet(resultset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
        
        /*
        transportLock.lock();
        try {
          if (stmtHandle != null) {
            TCancelOperationReq cancelReq = new TCancelOperationReq(stmtHandle);
            TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
            Utils.verifySuccessWithInfo(cancelResp.getStatus());
          }
        } catch (SQLException e) {
          throw e;
        } catch (Exception e) {
          throw new SQLException(e.toString(), "08S01", e);
        } finally {
          transportLock.unlock();
        }*/
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getSuperTables(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getSuperTypes(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String arg0, String arg1, String arg2)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resultset = client.connection_getTableTypes(connection);
            return new CrystalResultSet(resultset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
            String[] types) throws SQLException 
    {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resultset = client.connection_getTables(connection, catalog, schemaPattern, tableNamePattern, tabToList(types));
            return new CrystalResultSet(resultset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }
    
    private static List<String> tabToList(String[] types) {
        List<String> typesTab = null;
            if (types!=null) {
                typesTab = Arrays.asList(types);
            }
        return typesTab;
    }
    
    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            CCResultSet resultset = client.connection_getTypeInfo(connection);
            return new CrystalResultSet(resultset);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getURL() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getUserName() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean insertsAreDetected(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getReadOnly(connection);
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean othersDeletesAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean othersInsertsAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean othersUpdatesAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean ownDeletesAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean ownInsertsAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean ownUpdatesAreVisible(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).isSupportsCatalogsInTableDefinitions();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsConvert(int arg0, int arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsResultSetConcurrency(int arg0, int arg1)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsResultSetHoldability(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsResultSetType(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).isSupportsSavepoints();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).isSupportsSchemasInDataManipulation();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        Client client = null;
        try {
            client = this.crystalConnection.lockClient();
            return client.connection_getstaticmetadata(connection).isSupportsSchemasInTableDefinitions();
        } catch (CCSQLException e) {
            throw new SQLException(e.reason, e.sqlState, e.vendorCode, e);
        } catch (Exception e) {
            throw new SQLException(e.toString(), "08S01", e);
        } finally {
            this.crystalConnection.unlockClient(client);
        }
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int arg0)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean updatesAreDetected(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        throw new SQLException("Method not supported");
    }

}
