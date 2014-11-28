package driver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import driver.io.CCResultSetMetaData;

public class CrystalResultSetMetaData implements ResultSetMetaData {

    private CCResultSetMetaData metadata;

    public CrystalResultSetMetaData(CCResultSetMetaData metadata) {
        this.metadata = metadata;
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
    public String getCatalogName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).catalogName;
    }

    @Override
    public String getColumnClassName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnClassName;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.metadata.parts.size();
    }

    @Override
    public int getColumnDisplaySize(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnDisplaySize;
    }

    @Override
    public String getColumnLabel(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnLabel;
    }

    @Override
    public String getColumnName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnName;
    }

    @Override
    public int getColumnType(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnType;
    }

    @Override
    public String getColumnTypeName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).columnTypeName;
    }

    @Override
    public int getPrecision(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).precision;
    }

    @Override
    public int getScale(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).scale;
    }

    @Override
    public String getSchemaName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).schemaName;
    }

    @Override
    public String getTableName(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).tableName;
    }

    @Override
    public boolean isAutoIncrement(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).autoIncrement;
    }

    @Override
    public boolean isCaseSensitive(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).caseSensitive;
    }

    @Override
    public boolean isCurrency(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).currency;
    }

    @Override
    public boolean isDefinitelyWritable(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).definitelyWritable;
    }

    @Override
    public int isNullable(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).nullable;
    }

    @Override
    public boolean isReadOnly(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).readOnly;
    }

    @Override
    public boolean isSearchable(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).searchable;
    }

    @Override
    public boolean isSigned(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).signed;
    }

    @Override
    public boolean isWritable(int arg0) throws SQLException {
        return this.metadata.parts.get(arg0 - 1).writable;
    }

    public int findColumn(String columnLabel) {
        // Check columnLabel
        for (int i=0; i < this.metadata.parts.size(); i++)
        {
            if (columnLabel.equals(this.metadata.parts.get(i).columnLabel)) {
                return i+1;
            }
        }
        // Check columnName
        for (int i=0; i < this.metadata.parts.size(); i++)
        {
            if (columnLabel.equals(this.metadata.parts.get(i).columnName)) {
                return i+1;
            }
        }
        return -1;
    }

}
