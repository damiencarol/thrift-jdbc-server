
namespace * driver.io

exception InvalidOperation {
  1: i32 errorCode,
  2: string message
}

struct CCConnection
{
  1: i32 id
}

struct CCStatement
{
  1: i32 id, 
  2: string sql
  3: i32 id_connection
}

struct CCProperty
{
  1: string key,
  2: string value
}

union RawVal {
  1: i64 bigint_val,
  2: i32 integer_val,
  3: i16 smallint_val,
  4: double double_val,
  5: bool bool_val,
  6: string string_val
}

struct CCValue
{
  1: bool isnull,
  2: RawVal val
}

  
struct CCRow
{
  1: list<CCValue> values
}

struct CCResultSetMetaDataPart 
{
  1:  string catalogName,
  2:  string columnClassName,
  3:  i32 columnDisplaySize,
  4:  string columnLabel,
  5:  string columnName,
  6:  i32 columnType,
  7:  string columnTypeName,
  8:  i32 precision,
  9:  i32 scale,
  10:  string schemaName,
  11:  string tableName,
  12:  bool autoIncrement,
  13:  bool caseSensitive,
  14:  bool currency,
  15:  bool definitelyWritable,
  16:  i32 nullable,
  17:  bool readOnly,
  18:  bool searchable,
  19:  bool signed,
  20:  bool writable
}

struct CCResultSetMetaData
{
  1: list<CCResultSetMetaDataPart> parts
}

struct CCResultSet
{
  1: i32 id,
  2: list<CCRow> rows,
  3: CCResultSetMetaData metadata
}

struct CCStaticMetaData
{
  1: i32 databaseMajorVersion,
  2: i32 databaseMinorVersion,
  3: string databaseProductName,
  4: string databaseProductVersion,
  5: i32 defaultTransactionIsolation,
  6: string identifierQuoteString,
  7: bool supportsCatalogsInTableDefinitions,
  8: bool supportsSavepoints,
  9: bool supportsSchemasInDataManipulation,
  10: bool supportsSchemasInTableDefinitions
}

struct CCSQLWarning {
  1: string reason
  2: string state
  3: i32 vendorCode
}

struct statement_getWarnings_return {
  1: list<CCSQLWarning> warnings
}

exception CCSQLException {
  1: string reason
  2: string sqlState
  3: i32 vendorCode
}

service ConnectionService {

   CCConnection createConnection(1:string url, 2:map<string,string> properties),
   
   CCStatement createStatement(1:CCConnection connection),
   
   CCStaticMetaData connection_getstaticmetadata(1:CCConnection connection),
   bool connection_isvalid(1:CCConnection connection, 2:i32 timeout),
   
   void connection_setAutoCommit(1:CCConnection connection, 2:bool autoCommit) throws (1:CCSQLException ouch)
   bool connection_getAutoCommit(1:CCConnection connection) throws (1:CCSQLException ouch)
   void connection_setTransactionIsolation(1:CCConnection connection, 2:i32 level) throws (1:CCSQLException ouch)
   i32 connection_getTransactionIsolation(1:CCConnection connection) throws (1:CCSQLException ouch)
   void connection_setReadOnly(1:CCConnection connection, 2:bool readOnly) throws (1:CCSQLException ouch)
   bool connection_getReadOnly(1:CCConnection connection) throws (1:CCSQLException ouch)
   
   void connection_setCatalog(1:CCConnection connection, 2:string catalog) throws (1:CCSQLException ouch)
   string connection_getCatalog(1:CCConnection connection) throws (1:CCSQLException ouch)
   void connection_setSchema(1:CCConnection connection, 2:string schema) throws (1:CCSQLException ouch)
   string connection_getSchema(1:CCConnection connection) throws (1:CCSQLException ouch)
   
   string connection_getCatalogSeparator(1:CCConnection connection),
   string connection_getCatalogTerm(1:CCConnection connection),
   string connection_getSchemaTerm(1:CCConnection connection),
   
   CCResultSet connection_getCatalogs(1:CCConnection connection),
   CCResultSet connection_getSchemas(1:CCConnection connection, 2:string catalog, 3:string schemaPattern) throws (1:CCSQLException ouch)
   CCResultSet connection_getTables(1:CCConnection connection, 2:string catalog, 3:string schemaPattern, 4:string tableNamePattern, 5:list<string> types),
   CCResultSet connection_getColumns(1:CCConnection connection, 2:string catalog, 3:string schemaPattern, 4:string tableNamePattern, 5:string columnNamePattern),
   string connection_getSQLKeywords(1:CCConnection connection),
   CCResultSet connection_getTableTypes(1:CCConnection connection),
   
   CCResultSet connection_getTypeInfo(1:CCConnection connection) throws (1:CCSQLException ouch)

   void closeConnection(1:CCConnection connection) throws (1:CCSQLException ouch)
   
   void statement_close(1:CCStatement statement) throws (1:CCSQLException ouch)
   bool statement_execute(1:CCStatement statement, 2:string sql) throws (1:CCSQLException ouch)
   CCResultSet statement_executeQuery(1:CCStatement statement, 2:string sql) throws (1:CCSQLException ouch)
   CCResultSet statement_getResultSet(1:CCStatement statement) throws (1:CCSQLException ouch)
   i32 statement_getUpdateCount(1:CCStatement statement),
   i32 statement_getResultSetType(1:CCStatement statement)
   void statement_cancel(1:CCStatement statement) throws (1:CCSQLException ouch)
   
   statement_getWarnings_return statement_getWarnings(1:CCStatement statement) throws (1:CCSQLException ouch)
   void statement_clearWarnings(1:CCStatement statement) throws (1:CCSQLException ouch)
   
   i32 statement_getMaxRows(1:CCStatement statement) throws (1:CCSQLException ouch)
   void statement_setMaxRows(1:CCStatement statement, 2:i32 max) throws (1:CCSQLException ouch)
}