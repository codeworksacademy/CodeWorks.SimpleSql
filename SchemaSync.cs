using System.Reflection;
using System.Threading;
using System.Data;
using Dapper;

namespace CodeWorks.SimpleSql;

public static class SchemaSync
{
  private static readonly SemaphoreSlim SyncGate = new(1, 1);
  private static readonly object SyncedKeysGate = new();
  private static readonly HashSet<string> SyncedKeys = [];

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
    ISqlDialect? dialect = null,
    SchemaSyncOptions? options = null)
  {
    SchemaSyncLogger.Configure(options);

    EnsureTransactionConnectionMatches(db, tx);

    var activeDialect = dialect ?? SqlDialects.Detect(db);
    var activeSchema = schema ?? await DetectSchemaAsync(db, tx, activeDialect);
    var syncKey = BuildSyncKey(db, activeDialect, activeSchema);

    await SyncGate.WaitAsync();
    try
    {
      if (IsSynced(syncKey))
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
            activeSchema,
            model,
            tableName,
            activeDialect
          );
        }
      }

      SchemaSyncLogger.Write();

      var setSchemaSql = activeDialect.SetSchemaSql(activeSchema);
      if (!string.IsNullOrWhiteSpace(setSchemaSql))
      {
        await db.ExecuteAsync(setSchemaSql, transaction: tx);
      }

      MarkSynced(syncKey);
    }
    finally
    {
      SyncGate.Release();
    }
  }

  private static void EnsureTransactionConnectionMatches(IDbConnection db, IDbTransaction tx)
  {
    if (tx.Connection != null && !ReferenceEquals(tx.Connection, db))
      throw new InvalidOperationException("The provided transaction does not belong to the provided connection.");
  }

  private static string BuildSyncKey(IDbConnection db, ISqlDialect dialect, string schema)
  {
    var database = string.IsNullOrWhiteSpace(db.Database) ? "(unknown-db)" : db.Database;
    return $"{dialect.Name}|{database}|{schema}";
  }

  private static bool IsSynced(string key)
  {
    lock (SyncedKeysGate)
      return SyncedKeys.Contains(key);
  }

  private static void MarkSynced(string key)
  {
    lock (SyncedKeysGate)
      SyncedKeys.Add(key);
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

public sealed class SchemaSyncOptions
{
  public string? LogPath { get; init; }
  public bool EnableConsoleLogging { get; init; } = true;
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
      dialect.BuildTableExistsSql(),
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

      var addColumnSql = SearchVectorSchemaSqlBuilder.BuildAddColumnSql(
        type,
        prop,
        schema,
        tableName,
        dialect);

      await db.ExecuteAsync(addColumnSql, transaction: tx);

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

    foreach (var prop in model.GetProperties(BindingFlags.Public | BindingFlags.Instance))
    {
      var searchVector = prop.GetCustomAttribute<DbSearchVectorAttribute>();
      if (searchVector == null || !searchVector.CreateIndex)
        continue;

      var column = prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName
        ?? SqlHelper.ToSnakeCase(prop.Name)!;

      var indexName = dialect.BuildIndexName(table, [column]);
      var exists = await db.ExecuteScalarAsync<bool>(
        dialect.BuildIndexExistsSql(),
        new { schema, table, index = indexName },
        tx
      );

      if (exists)
        continue;

      var sql = SearchVectorSchemaSqlBuilder.BuildIndexSql(
        model,
        prop,
        schema,
        table,
        dialect,
        indexName);

      if (string.IsNullOrWhiteSpace(sql))
        continue;

      await db.ExecuteAsync(sql, transaction: tx);
      SchemaSyncLogger.Log($"[New Search Index Added]: {table}.{indexName}");
    }
  }
}

internal static class SearchVectorSchemaSqlBuilder
{
  private readonly record struct SearchVectorSourceSpec(string Column, string? Weight);

  public static string BuildAddColumnSql(
    Type modelType,
    PropertyInfo prop,
    string schema,
    string tableName,
    ISqlDialect dialect)
  {
    var column = prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName
      ?? SqlHelper.ToSnakeCase(prop.Name)
      ?? prop.Name;

    var searchVector = prop.GetCustomAttribute<DbSearchVectorAttribute>();
    if (searchVector == null || !string.Equals(dialect.Name, "postgres", StringComparison.OrdinalIgnoreCase))
    {
      var sqlType = dialect.BuildSqlType(prop.PropertyType, prop);
      return dialect.BuildAddColumnSql(schema, tableName, column, sqlType);
    }

    var expression = BuildPostgresExpression(modelType, prop, searchVector, dialect);
    var full = dialect.BuildTableName(schema, tableName);
    return $"ALTER TABLE {full} ADD COLUMN {dialect.Quote(column)} TSVECTOR GENERATED ALWAYS AS ({expression}) STORED;";
  }

  public static string? BuildIndexSql(
    Type modelType,
    PropertyInfo prop,
    string schema,
    string tableName,
    ISqlDialect dialect,
    string indexName)
  {
    _ = modelType;

    var searchVector = prop.GetCustomAttribute<DbSearchVectorAttribute>();
    if (searchVector == null || !searchVector.CreateIndex)
      return null;

    if (!string.Equals(dialect.Name, "postgres", StringComparison.OrdinalIgnoreCase))
      return null;

    var column = prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName
      ?? SqlHelper.ToSnakeCase(prop.Name)
      ?? prop.Name;

    var full = dialect.BuildTableName(schema, tableName);
    return $"CREATE INDEX IF NOT EXISTS {dialect.Quote(indexName)} ON {full} USING GIN ({dialect.Quote(column)});";
  }

  private static string BuildPostgresExpression(
    Type modelType,
    PropertyInfo prop,
    DbSearchVectorAttribute searchVector,
    ISqlDialect dialect)
  {
    var configuration = (searchVector.Configuration ?? "simple").Replace("'", "''");

    var vectorExpressions = ResolveSourceColumns(modelType, prop, searchVector)
      .Select(source => BuildSourceVectorExpression(source, configuration, dialect))
      .ToArray();

    if (vectorExpressions.Length == 0)
      return $"to_tsvector('{configuration}', '')";

    return string.Join(" || ", vectorExpressions);
  }

  private static string BuildSourceVectorExpression(
    SearchVectorSourceSpec source,
    string configuration,
    ISqlDialect dialect)
  {
    var textExpression = $"COALESCE(CAST({dialect.Quote(source.Column)} AS TEXT), '')";
    var vectorExpression = $"to_tsvector('{configuration}', {textExpression})";

    return string.IsNullOrWhiteSpace(source.Weight)
      ? vectorExpression
      : $"setweight({vectorExpression}, '{source.Weight}')";
  }

  private static IReadOnlyList<SearchVectorSourceSpec> ResolveSourceColumns(
    Type modelType,
    PropertyInfo prop,
    DbSearchVectorAttribute searchVector)
  {
    var sourceNames = searchVector.SourceProperties;
    var sourceWeights = searchVector.SourceWeights;

    if (sourceNames.Length == 0)
    {
      return modelType
        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
        .Where(candidate =>
          candidate.Name != prop.Name &&
          candidate.PropertyType == typeof(string) &&
          candidate.GetCustomAttribute<DbSearchVectorAttribute>() == null)
        .Select((candidate, index) =>
        {
          var column = candidate.GetCustomAttribute<DbColumnAttribute>()?.ColumnName ?? SqlHelper.ToSnakeCase(candidate.Name) ?? candidate.Name;
          var weight = ResolveWeight(sourceWeights, index, modelType, prop.Name);
          return new SearchVectorSourceSpec(column, weight);
        })
        .ToArray();
    }

    return sourceNames
      .Select((sourceName, index) =>
      {
        var sourceProp = modelType.GetProperty(sourceName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)
          ?? throw new InvalidOperationException($"Could not resolve DbSearchVector source property '{sourceName}' on {modelType.Name}.");

        var column = sourceProp.GetCustomAttribute<DbColumnAttribute>()?.ColumnName ?? SqlHelper.ToSnakeCase(sourceProp.Name) ?? sourceProp.Name;
        var weight = ResolveWeight(sourceWeights, index, modelType, prop.Name);
        return new SearchVectorSourceSpec(column, weight);
      })
      .ToArray();
  }

  private static string? ResolveWeight(
    IReadOnlyList<string> weights,
    int index,
    Type modelType,
    string propertyName)
  {
    if (weights.Count <= index || string.IsNullOrWhiteSpace(weights[index]))
      return null;

    var weight = weights[index].Trim().ToUpperInvariant();
    if (weight is not ("A" or "B" or "C" or "D"))
      throw new InvalidOperationException($"Invalid DbSearchVector source weight '{weights[index]}' on {modelType.Name}.{propertyName}. Use A, B, C, or D.");

    return weight;
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
  private static readonly object Gate = new();
  private static string? _logPath;
  private static bool _enableConsoleLogging = true;

  private static readonly List<string> PendingLogs = [];

  public static void Configure(SchemaSyncOptions? options)
  {
    if (options == null)
      return;

    lock (Gate)
    {
      _logPath = string.IsNullOrWhiteSpace(options.LogPath) ? null : options.LogPath;
      _enableConsoleLogging = options.EnableConsoleLogging;
    }
  }

  public static void Log(string message)
  {
    var line = $"[SchemaSync] [{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] {message}";
    lock (Gate)
    {
      PendingLogs.Add(line);

      if (_enableConsoleLogging)
        Console.WriteLine(line);
    }
  }

  public static void Write()
  {
    List<string> snapshot;
    string? logPath;
    bool writeToConsole;

    lock (Gate)
    {
      writeToConsole = _enableConsoleLogging;

      if (PendingLogs.Count == 0)
      {
        if (writeToConsole)
          Console.WriteLine("[SchemaSync] No logs to write.");

        return;
      }

      snapshot = PendingLogs.ToList();
      PendingLogs.Clear();
      logPath = _logPath;
    }

    if (!string.IsNullOrWhiteSpace(logPath))
    {
      var logDir = Path.GetDirectoryName(logPath);
      if (!string.IsNullOrWhiteSpace(logDir) && !Directory.Exists(logDir))
        Directory.CreateDirectory(logDir);

      File.AppendAllLines(logPath, snapshot);
    }

    if (writeToConsole)
    {
      if (!string.IsNullOrWhiteSpace(logPath))
        Console.WriteLine($"[SchemaSync] Wrote {snapshot.Count} log entries to {logPath}");
      else
        Console.WriteLine($"[SchemaSync] Processed {snapshot.Count} log entries.");
    }
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
