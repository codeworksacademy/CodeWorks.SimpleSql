using System.Reflection;
using System.Threading;
using System.Data;
using Dapper;

namespace CodeWorks.SimpleSql;

public static class SchemaSync
{
  private static int _synced = 0;
  private static string? _schema;

  private static readonly ISchemaSyncStep[] Steps =
  [
    new TableSyncStep(),
    new ColumnSyncStep(),
    new IndexSyncStep(),
    new ConstraintSyncStep()
  ];

  public static async Task SyncModelsAsync(
    IDbConnection db,
    IDbTransaction tx,
    IEnumerable<Type> models,
    string? schema = null,
    ISqlDialect? dialect = null)
  {
    var activeDialect = dialect ?? SqlDialects.Detect(db);
    _schema ??= schema ?? await DetectSchemaAsync(db, tx, activeDialect);

    if (Interlocked.Exchange(ref _synced, 1) == 1)
    {
      Console.WriteLine("[SchemaSync] Already synced.");
      return;
    }

    foreach (var model in models)
    {
      var tableAttr = model.GetCustomAttribute<DbTableAttribute>();
      if (tableAttr == null)
        continue;

      var tableName = tableAttr.TableName;

      foreach (var step in Steps)
      {
        await step.ExecuteAsync(
          db,
          tx,
          _schema,
          model,
          tableName,
          activeDialect
        );
      }
    }

    SchemaSyncLogger.Write();

    var setSchemaSql = activeDialect.SetSchemaSql(_schema);
    if (!string.IsNullOrWhiteSpace(setSchemaSql))
    {
      await db.ExecuteAsync(setSchemaSql, transaction: tx);
    }
  }

  private static async Task<string> DetectSchemaAsync(IDbConnection db, IDbTransaction tx, ISqlDialect dialect)
  {
    var schema = await db.ExecuteScalarAsync<string>(
      dialect.CurrentSchemaSql,
      transaction: tx
    );

    if (string.IsNullOrWhiteSpace(schema))
      throw new Exception("Failed to detect database schema.");

    return schema;
  }
}

internal sealed class TableSyncStep : ISchemaSyncStep
{
  public async Task ExecuteAsync(
    IDbConnection db,
    IDbTransaction tx,
    string schema,
    Type modelType,
    string tableName,
    ISqlDialect dialect)
  {
    var exists = await db.ExecuteScalarAsync<bool>(
      @"SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema = @schema
          AND table_name = @table
        )",
      new { schema, table = tableName },
      tx
    );

    if (exists) return;

    var sql = dialect.BuildCreateTableSql(schema, tableName);

    await db.ExecuteAsync(sql, transaction: tx);

    SchemaSyncLogger.Log($"[New Table Created]: {schema}.{tableName}");
  }
}

internal sealed class ColumnSyncStep : ISchemaSyncStep
{
  public async Task ExecuteAsync(
    IDbConnection db,
    IDbTransaction tx,
    string schema,
    Type type,
    string tableName,
    ISqlDialect dialect)
  {
    var properties = type
      .GetProperties(BindingFlags.Public | BindingFlags.Instance)
      .Where(p =>
          p.GetCustomAttribute<IgnoreWriteAttribute>() == null &&
          p.GetCustomAttribute<IgnoreSelectAttribute>() == null);

    var existing = (await db.QueryAsync<string>(
      @"SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=@schema
        AND table_name=@table",
      new { schema, table = tableName },
      tx))
      .ToHashSet(StringComparer.OrdinalIgnoreCase);

    foreach (var prop in properties)
    {
      var column =
        prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName
        ?? SqlHelper.ToSnakeCase(prop.Name)!;

      if (existing.Contains(column))
        continue;

      var sqlType = dialect.BuildSqlType(prop.PropertyType, prop);

      await db.ExecuteAsync(
        dialect.BuildAddColumnSql(schema, tableName, column, sqlType),
        transaction: tx);

      SchemaSyncLogger.Log($"[New Column Added]: {tableName}.{column}");
    }
  }
}

internal sealed class IndexSyncStep : ISchemaSyncStep
{
  public async Task ExecuteAsync(
    IDbConnection db,
    IDbTransaction tx,
    string schema,
    Type model,
    string table,
    ISqlDialect dialect)
  {
    var indexes = model.GetCustomAttributes<DbIndexAttribute>();

    foreach (var idx in indexes)
    {
      var indexName = dialect.BuildIndexName(table, idx.Columns);
      var exists = await db.ExecuteScalarAsync<bool>(
        dialect.BuildIndexExistsSql(),
        new { schema, table, index = indexName },
        tx
      );

      if (exists)
        continue;

      var sql = dialect.BuildCreateIndexSql(schema, table, indexName, idx.Columns, idx.Unique);
      await db.ExecuteAsync(sql, transaction: tx);
      SchemaSyncLogger.Log($"[New Index Added]: {table}.{indexName}");
    }
  }
}

internal sealed class ConstraintSyncStep : ISchemaSyncStep
{
  public async Task ExecuteAsync(
    IDbConnection db,
    IDbTransaction tx,
    string schema,
    Type model,
    string table,
    ISqlDialect dialect)
  {
    var constraints = model.GetCustomAttributes<DbConstraintAttribute>();

    foreach (var constraint in constraints)
    {
      var name = TryExtractConstraintName(constraint.Sql);
      if (!string.IsNullOrWhiteSpace(name))
      {
        var exists = await db.ExecuteScalarAsync<bool>(
          dialect.BuildConstraintExistsSql(),
          new { schema, table, constraint = name },
          tx
        );

        if (exists)
          continue;
      }

      var tableName = dialect.BuildTableName(schema, table);
      var sql = $"ALTER TABLE {tableName} ADD {constraint.Sql};";
      await db.ExecuteAsync(sql, transaction: tx);
      SchemaSyncLogger.Log($"[New Constraint Added]: {table}.{name ?? "(unknown)"}");
    }
  }

  private static string? TryExtractConstraintName(string sql)
  {
    var normalized = sql.Trim();
    var marker = "CONSTRAINT ";
    var start = normalized.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
    if (start < 0)
      return null;

    var tail = normalized[(start + marker.Length)..].TrimStart();
    if (tail.Length == 0)
      return null;

    var firstToken = tail.Split([' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
    return string.IsNullOrWhiteSpace(firstToken) ? null : firstToken.Trim('"', '[', ']');
  }
}

internal static class SchemaSyncLogger
{
  private static readonly string LogPath =
    Path.Combine(AppContext.BaseDirectory, "../../../logs", "db-sync.log");

  private static readonly List<string> PendingLogs = [];

  public static void Log(string message)
  {
    var line = $"[SchemaSync] [{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] {message}";
    PendingLogs.Add(line);
    Console.WriteLine(line);
  }

  public static void Write()
  {
    if (PendingLogs.Count == 0)
    {
      Console.WriteLine("[SchemaSync] No logs to write.");
      return;
    }

    var logDir = Path.GetDirectoryName(LogPath);
    if (!Directory.Exists(logDir))
      Directory.CreateDirectory(logDir!);

    File.AppendAllLines(LogPath, PendingLogs);
    Console.WriteLine($"[SchemaSync] Wrote {PendingLogs.Count} log entries to {LogPath}");
    PendingLogs.Clear();
  }
}

internal interface ISchemaSyncStep
{
  Task ExecuteAsync(
    IDbConnection db,
    IDbTransaction tx,
    string schema,
    Type modelType,
    string tableName,
    ISqlDialect dialect);
}
