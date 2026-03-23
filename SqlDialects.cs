using System.Data;
using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace CodeWorks.SimpleSql;

public interface ISqlDialect
{
  string Name { get; }
  string Quote(string identifier);
  string ParameterJsonCast(string parameterName);
  string ParameterVectorCast(string parameterName, DbVectorAttribute vector);
  string CastToText(string expressionSql);
  string CurrentSchemaSql { get; }
  string SetSchemaSql(string schema);
  string BuildTableName(string schema, string tableName);
  string BuildIndexName(string tableName, IEnumerable<string> columns);
  string BuildCreateTableSql(string schema, string tableName);
  string BuildTableExistsSql();
  string BuildAddColumnSql(string schema, string tableName, string columnName, string sqlType);
  string BuildCreateIndexSql(string schema, string tableName, string indexName, IEnumerable<string> columns, bool unique);
  string BuildConstraintExistsSql();
  string BuildIndexExistsSql();
  string BuildSqlType(Type clrType, PropertyInfo prop);
}

public static class SqlDialects
{
  public static readonly ISqlDialect Postgres = new PostgresDialect();
  public static readonly ISqlDialect SqlServer = new SqlServerDialect();

  public static ISqlDialect Detect(IDbConnection db)
  {
    var typeName = db.GetType().FullName ?? db.GetType().Name;

    if (typeName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase))
      return Postgres;

    if (typeName.Contains("SqlClient", StringComparison.OrdinalIgnoreCase) ||
        typeName.Contains("Microsoft.Data.SqlClient", StringComparison.OrdinalIgnoreCase) ||
        typeName.Contains("System.Data.SqlClient", StringComparison.OrdinalIgnoreCase))
      return SqlServer;

    return Postgres;
  }
}

internal sealed class PostgresDialect : ISqlDialect
{
  public string Name => "postgres";

  public string Quote(string identifier) => $"\"{identifier}\"";

  public string ParameterJsonCast(string parameterName) => $"{parameterName}::jsonb";

  public string ParameterVectorCast(string parameterName, DbVectorAttribute vector)
  {
    var sqlType = ResolveVectorSqlType(vector);
    var castTarget = sqlType.Split('(', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)[0];
    return $"{parameterName}::{castTarget}";
  }

  public string CastToText(string expressionSql) => $"CAST({expressionSql} AS TEXT)";

  public string CurrentSchemaSql => "SELECT current_schema();";

  public string SetSchemaSql(string schema) => $"SET search_path TO {Quote(schema)};";

  public string BuildTableName(string schema, string tableName) => $"{Quote(schema)}.{Quote(tableName)}";

  public string BuildIndexName(string tableName, IEnumerable<string> columns) => $"idx_{tableName}_{string.Join("_", columns)}";

  public string BuildCreateTableSql(string schema, string tableName)
  {
    var full = BuildTableName(schema, tableName);
    return $@"
      CREATE TABLE {full} (
        {Quote("id")} UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        {Quote("created_at")} TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        {Quote("updated_at")} TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );";
  }

  public string BuildTableExistsSql() => @"
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = @schema
      AND table_name = @table
    );";

  public string BuildAddColumnSql(string schema, string tableName, string columnName, string sqlType)
  {
    var full = BuildTableName(schema, tableName);
    return $@"ALTER TABLE {full} ADD COLUMN {Quote(columnName)} {sqlType};";
  }

  public string BuildCreateIndexSql(string schema, string tableName, string indexName, IEnumerable<string> columns, bool unique)
  {
    var full = BuildTableName(schema, tableName);
    var cols = string.Join(", ", columns.Select(Quote));
    var uniqueSql = unique ? "UNIQUE " : string.Empty;

    return $@"CREATE {uniqueSql}INDEX IF NOT EXISTS {Quote(indexName)} ON {full} ({cols});";
  }

  public string BuildConstraintExistsSql() => @"
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.table_constraints
      WHERE table_schema = @schema
      AND table_name = @table
      AND constraint_name = @constraint
    );";

  public string BuildIndexExistsSql() => @"
    SELECT EXISTS (
      SELECT 1
      FROM pg_indexes
      WHERE schemaname = @schema
      AND tablename = @table
      AND indexname = @index
    );";

  public string BuildSqlType(Type clrType, PropertyInfo prop)
  {
    if (prop.GetCustomAttribute<JsonColumnAttribute>() != null)
      return "JSONB";

    if (prop.GetCustomAttribute<DbSearchVectorAttribute>() != null)
      return "TSVECTOR";

    var vector = prop.GetCustomAttribute<DbVectorAttribute>();
    if (vector != null)
      return ResolveVectorSqlType(vector);

    var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

    if (type.IsEnum)
      return "TEXT";

    if (type == typeof(string))
    {
      var maxLength = prop.GetCustomAttribute<MaxLengthAttribute>()?.Length;
      if (maxLength.HasValue && maxLength.Value > 0)
        return $"VARCHAR({maxLength.Value})";
      return "TEXT";
    }

    if (type == typeof(int)) return "INTEGER";
    if (type == typeof(long)) return "BIGINT";
    if (type == typeof(short)) return "SMALLINT";
    if (type == typeof(byte)) return "SMALLINT";
    if (type == typeof(decimal)) return "NUMERIC";
    if (type == typeof(double)) return "DOUBLE PRECISION";
    if (type == typeof(float)) return "REAL";
    if (type == typeof(bool)) return "BOOLEAN";
    if (type == typeof(DateTime)) return "TIMESTAMP WITH TIME ZONE";
    if (type == typeof(DateTimeOffset)) return "TIMESTAMP WITH TIME ZONE";
    if (type == typeof(TimeSpan)) return "INTERVAL";
    if (type == typeof(DateOnly)) return "DATE";
    if (type == typeof(TimeOnly)) return "TIME";
    if (type == typeof(Guid)) return "UUID";

    return "TEXT";
  }

  private static string ResolveVectorSqlType(DbVectorAttribute vector)
  {
    if (!string.IsNullOrWhiteSpace(vector.SqlType))
      return vector.SqlType!;

    return vector.Dimensions.HasValue
      ? $"VECTOR({vector.Dimensions.Value})"
      : "VECTOR";
  }
}

internal sealed class SqlServerDialect : ISqlDialect
{
  public string Name => "sqlserver";

  public string Quote(string identifier) => $"[{identifier}]";

  public string ParameterJsonCast(string parameterName) => parameterName;

  public string ParameterVectorCast(string parameterName, DbVectorAttribute vector) => parameterName;

  public string CastToText(string expressionSql) => $"CAST({expressionSql} AS NVARCHAR(MAX))";

  public string CurrentSchemaSql => "SELECT SCHEMA_NAME();";

  public string SetSchemaSql(string schema) => string.Empty;

  public string BuildTableName(string schema, string tableName) => $"{Quote(schema)}.{Quote(tableName)}";

  public string BuildIndexName(string tableName, IEnumerable<string> columns) => $"idx_{tableName}_{string.Join("_", columns)}";

  public string BuildCreateTableSql(string schema, string tableName)
  {
    var full = BuildTableName(schema, tableName);
    return $@"
      CREATE TABLE {full} (
        {Quote("id")} UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        {Quote("created_at")} DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        {Quote("updated_at")} DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      );";
  }

  public string BuildTableExistsSql() => @"
    SELECT CASE WHEN EXISTS (
      SELECT 1 FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = @schema
      AND TABLE_NAME = @table
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END;";

  public string BuildAddColumnSql(string schema, string tableName, string columnName, string sqlType)
  {
    var full = BuildTableName(schema, tableName);
    return $@"ALTER TABLE {full} ADD {Quote(columnName)} {sqlType};";
  }

  public string BuildCreateIndexSql(string schema, string tableName, string indexName, IEnumerable<string> columns, bool unique)
  {
    var full = BuildTableName(schema, tableName);
    var cols = string.Join(", ", columns.Select(Quote));
    var uniqueSql = unique ? "UNIQUE " : string.Empty;

    return $@"CREATE {uniqueSql}INDEX {Quote(indexName)} ON {full} ({cols});";
  }

  public string BuildConstraintExistsSql() => @"
    SELECT CASE WHEN EXISTS (
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @schema
      AND TABLE_NAME = @table
      AND CONSTRAINT_NAME = @constraint
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END;";

  public string BuildIndexExistsSql() => @"
    SELECT CASE WHEN EXISTS (
      SELECT 1
      FROM sys.indexes i
      JOIN sys.tables t ON i.object_id = t.object_id
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @schema
      AND t.name = @table
      AND i.name = @index
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END;";

  public string BuildSqlType(Type clrType, PropertyInfo prop)
  {
    if (prop.GetCustomAttribute<JsonColumnAttribute>() != null)
      return "NVARCHAR(MAX)";

    if (prop.GetCustomAttribute<DbSearchVectorAttribute>() != null)
      return "NVARCHAR(MAX)";

    var vector = prop.GetCustomAttribute<DbVectorAttribute>();
    if (vector != null)
      return !string.IsNullOrWhiteSpace(vector.SqlType)
        ? vector.SqlType!
        : "NVARCHAR(MAX)";

    var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

    if (type.IsEnum)
      return "NVARCHAR(64)";

    if (type == typeof(string))
    {
      var maxLength = prop.GetCustomAttribute<MaxLengthAttribute>()?.Length;
      if (maxLength.HasValue && maxLength.Value > 0)
        return $"NVARCHAR({maxLength.Value})";
      return "NVARCHAR(MAX)";
    }

    if (type == typeof(int)) return "INT";
    if (type == typeof(long)) return "BIGINT";
    if (type == typeof(short)) return "SMALLINT";
    if (type == typeof(byte)) return "TINYINT";
    if (type == typeof(decimal)) return "DECIMAL(18, 6)";
    if (type == typeof(double)) return "FLOAT";
    if (type == typeof(float)) return "REAL";
    if (type == typeof(bool)) return "BIT";
    if (type == typeof(DateTime)) return "DATETIME2";
    if (type == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
    if (type == typeof(TimeSpan)) return "TIME";
    if (type == typeof(DateOnly)) return "DATE";
    if (type == typeof(TimeOnly)) return "TIME";
    if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";

    return "NVARCHAR(MAX)";
  }
}
