using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Dapper;

namespace CodeWorks.SimpleSql;

public interface ISqlSession
{
  IDbSet<T> Set<T>();
}

public interface IDbSet<T>
{
  IDbSet<T> Where(Expression<Func<T, bool>> predicate);
  IDbSet<T> Include<TJoin>(Expression<Func<T, object?>> navigation, SqlJoinType joinType = SqlJoinType.Left, string? alias = null);
  IDbSet<T> OrderBy(Expression<Func<T, object>> expression, bool desc = false);
  IDbSet<T> Page(int page, int size);
  Task<List<T>> ToListAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<T?> FirstOrDefaultAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<int> CountAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<bool> AnyAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<int> UpsertAsync(T entity, Expression<Func<T, object>> keySelector, IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<int> UpsertManyAsync(IEnumerable<T> entities, Expression<Func<T, object>> keySelector, int batchSize = 200, IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  CompiledQuery ToUpsertCompiledQuery(T entity, Expression<Func<T, object>> keySelector);
  CompiledQuery ToCompiledQuery();
  CompiledQuery ToCompiledQuery(SqlQueryResultMode mode);
}

public sealed class SqlSession : ISqlSession
{
  private readonly IDbConnection _db;
  private readonly ISqlDialect _dialect;

  public SqlSession(IDbConnection db, ISqlDialect? dialect = null)
  {
    _db = db;
    _dialect = dialect ?? SqlDialects.Detect(db);
  }

  public IDbSet<T> Set<T>() => new DbSet<T>(_db, _dialect);
}

public sealed class DbSet<T> : IDbSet<T>
{
  private readonly IDbConnection _db;
  private readonly ISqlDialect _dialect;
  private readonly SqlQueryModel _model;

  public DbSet(IDbConnection db, ISqlDialect? dialect = null)
  {
    _db = db;
    _dialect = dialect ?? SqlDialects.Detect(db);
    _model = SqlQueryModel.Create(typeof(T));
  }

  private DbSet(IDbConnection db, ISqlDialect dialect, SqlQueryModel model)
  {
    _db = db;
    _dialect = dialect;
    _model = model;
  }

  public IDbSet<T> Where(Expression<Func<T, bool>> predicate)
    => new DbSet<T>(_db, _dialect, _model.AddWhere(predicate));

  public IDbSet<T> Include<TJoin>(
    Expression<Func<T, object?>> navigation,
    SqlJoinType joinType = SqlJoinType.Left,
    string? alias = null)
    => new DbSet<T>(_db, _dialect, _model.AddInclude(typeof(TJoin), navigation, joinType, alias));

  public IDbSet<T> OrderBy(Expression<Func<T, object>> expression, bool desc = false)
    => new DbSet<T>(_db, _dialect, _model.AddOrder(expression, desc));

  public IDbSet<T> Page(int page, int size)
  {
    if (page < 1) throw new ArgumentOutOfRangeException(nameof(page));
    if (size < 1) throw new ArgumentOutOfRangeException(nameof(size));

    return new DbSet<T>(_db, _dialect, _model.SetPaging(page, size));
  }

  public CompiledQuery ToCompiledQuery() => ToCompiledQuery(SqlQueryResultMode.List);

  public CompiledQuery ToCompiledQuery(SqlQueryResultMode mode) => SqlQueryCompiler.Compile(_model, _dialect, mode);

  public async Task<List<T>> ToListAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    var compiled = ToCompiledQuery();
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    var rows = await _db.QueryAsync<T>(command);
    return rows.ToList();
  }

  public async Task<T?> FirstOrDefaultAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    var compiled = ToCompiledQuery(SqlQueryResultMode.First);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    return await _db.QueryFirstOrDefaultAsync<T>(command);
  }

  public async Task<int> CountAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    var compiled = ToCompiledQuery(SqlQueryResultMode.Count);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    return await _db.ExecuteScalarAsync<int>(command);
  }

  public async Task<bool> AnyAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    var compiled = ToCompiledQuery(SqlQueryResultMode.Exists);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    return await _db.ExecuteScalarAsync<bool>(command);
  }

  public CompiledQuery ToUpsertCompiledQuery(T entity, Expression<Func<T, object>> keySelector)
    => SqlWriteCompiler.BuildUpsert(entity, keySelector, _dialect);

  public async Task<int> UpsertAsync(
    T entity,
    Expression<Func<T, object>> keySelector,
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    var compiled = ToUpsertCompiledQuery(entity, keySelector);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    return await _db.ExecuteAsync(command);
  }

  public async Task<int> UpsertManyAsync(
    IEnumerable<T> entities,
    Expression<Func<T, object>> keySelector,
    int batchSize = 200,
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(entities);
    if (batchSize < 1)
      throw new ArgumentOutOfRangeException(nameof(batchSize));

    var rows = entities as IList<T> ?? entities.ToList();
    if (rows.Count == 0)
      return 0;

    var plan = SqlWriteCompiler.BuildUpsertPlan(keySelector, _dialect);
    var total = 0;

    foreach (var batch in rows.Chunk(batchSize))
    {
      var parameterBatch = batch
        .Select(row => (object)SqlWriteCompiler.BuildUpsertParameters(row, plan.InsertProps))
        .ToList();

      var command = new CommandDefinition(
        plan.Sql,
        parameterBatch,
        transaction: transaction,
        cancellationToken: cancellationToken);

      total += await _db.ExecuteAsync(command);
    }

    return total;
  }
}

public readonly record struct CompiledQuery(string Sql, DynamicParameters Parameters);

public enum SqlQueryResultMode
{
  List,
  First,
  Count,
  Exists
}

internal readonly record struct IncludeSpec(
  Type JoinType,
  LambdaExpression Navigation,
  SqlJoinType JoinKind,
  string? Alias);

internal readonly record struct OrderSpec(LambdaExpression Expression, bool Desc);

internal sealed class SqlQueryModel
{
  public Type RootType { get; }
  public IReadOnlyList<LambdaExpression> Wheres { get; }
  public IReadOnlyList<IncludeSpec> Includes { get; }
  public IReadOnlyList<OrderSpec> Orders { get; }
  public int? Limit { get; }
  public int? Offset { get; }

  private SqlQueryModel(
    Type rootType,
    IReadOnlyList<LambdaExpression>? wheres = null,
    IReadOnlyList<IncludeSpec>? includes = null,
    IReadOnlyList<OrderSpec>? orders = null,
    int? limit = null,
    int? offset = null)
  {
    RootType = rootType;
    Wheres = wheres ?? [];
    Includes = includes ?? [];
    Orders = orders ?? [];
    Limit = limit;
    Offset = offset;
  }

  public static SqlQueryModel Create(Type rootType) => new(rootType);

  public SqlQueryModel AddWhere(LambdaExpression expression)
  {
    var next = Wheres.ToList();
    next.Add(expression);
    return new SqlQueryModel(RootType, next, Includes, Orders, Limit, Offset);
  }

  public SqlQueryModel AddInclude(Type joinType, LambdaExpression navigation, SqlJoinType joinKind, string? alias)
  {
    var next = Includes.ToList();
    next.Add(new IncludeSpec(joinType, navigation, joinKind, alias));
    return new SqlQueryModel(RootType, Wheres, next, Orders, Limit, Offset);
  }

  public SqlQueryModel AddOrder(LambdaExpression expression, bool desc)
  {
    var next = Orders.ToList();
    next.Add(new OrderSpec(expression, desc));
    return new SqlQueryModel(RootType, Wheres, Includes, next, Limit, Offset);
  }

  public SqlQueryModel SetPaging(int page, int size)
  {
    var offset = (page - 1) * size;
    return new SqlQueryModel(RootType, Wheres, Includes, Orders, size, offset);
  }
}

internal static class SqlQueryCompiler
{
  public static CompiledQuery Compile(
    SqlQueryModel model,
    ISqlDialect dialect,
    SqlQueryResultMode mode = SqlQueryResultMode.List)
  {
    var parameters = new DynamicParameters();
    var query = CreateTypedQuery(model.RootType, dialect);

    foreach (var include in model.Includes)
      query = ApplyInclude(query, include);

    var fromSql = (string)(query.GetType()
      .GetMethod(nameof(SqlQuery<object>.BuildFrom))
      ?.Invoke(query, null)
      ?? throw new InvalidOperationException("Failed to build FROM SQL."));

    var selectSql = (string)(query.GetType()
      .GetMethod(nameof(SqlQuery<object>.BuildSelect))
      ?.Invoke(query, null)
      ?? throw new InvalidOperationException("Failed to build SELECT SQL."));

    var whereParts = model.Wheres
      .Select(w => SqlExpressionBuilder.Build(w, SQLMapper.Get(model.RootType), parameters, "t0", dialect).Replace("WHERE ", string.Empty))
      .Where(w => !string.IsNullOrWhiteSpace(w))
      .ToList();

    var whereSql = whereParts.Count == 0
      ? string.Empty
      : " WHERE " + string.Join(" AND ", whereParts);

    var orderParts = model.Orders
      .Select(o => SqlHelper.BuildOrderBy(o.Expression, model.RootType, "t0", o.Desc, dialect))
      .ToList();

    var orderSql = orderParts.Count == 0
      ? string.Empty
      : " ORDER BY " + string.Join(", ", orderParts);

    var pagingSql = SqlHelper.BuildPaging(model.Limit, model.Offset);
    var paging = string.IsNullOrWhiteSpace(pagingSql) ? string.Empty : " " + pagingSql;

    var baseQuerySql = $"{selectSql} {fromSql}{whereSql}";
    var sql = mode switch
    {
      SqlQueryResultMode.List => $"{baseQuerySql}{orderSql}{paging}",
      SqlQueryResultMode.First => BuildFirstSql(baseQuerySql, orderSql, dialect),
      SqlQueryResultMode.Count => $"SELECT COUNT(1) {fromSql}{whereSql}",
      SqlQueryResultMode.Exists => BuildExistsSql(fromSql, whereSql, dialect),
      _ => throw new NotSupportedException($"Unsupported query mode: {mode}")
    };

    return new CompiledQuery(sql, parameters);
  }

  private static string BuildFirstSql(string baseQuerySql, string orderSql, ISqlDialect dialect)
  {
    if (string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
    {
      const string token = "SELECT ";
      return baseQuerySql.StartsWith(token, StringComparison.Ordinal)
        ? $"SELECT TOP 1 {baseQuerySql[token.Length..]}{orderSql}"
        : $"SELECT TOP 1 * FROM ({baseQuerySql}) t{orderSql}";
    }

    return $"{baseQuerySql}{orderSql} LIMIT 1";
  }

  private static string BuildExistsSql(string fromSql, string whereSql, ISqlDialect dialect)
  {
    if (string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
      return $"SELECT CASE WHEN EXISTS (SELECT 1 {fromSql}{whereSql}) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END";

    return $"SELECT EXISTS (SELECT 1 {fromSql}{whereSql})";
  }

  private static object CreateTypedQuery(Type rootType, ISqlDialect dialect)
  {
    var queryType = typeof(SqlQuery<>).MakeGenericType(rootType);
    return Activator.CreateInstance(queryType, "t0", dialect)
      ?? throw new InvalidOperationException($"Failed to create SqlQuery for type {rootType.Name}.");
  }

  private static object ApplyInclude(object query, IncludeSpec include)
  {
    var queryType = query.GetType();
    var method = queryType
      .GetMethods(BindingFlags.Public | BindingFlags.Instance)
      .FirstOrDefault(m =>
        m.Name == nameof(SqlQuery<object>.Include)
        && m.IsGenericMethodDefinition
        && m.GetParameters().Length == 3)
      ?? throw new InvalidOperationException("Could not locate Include method.");

    var genericInclude = method.MakeGenericMethod(include.JoinType);

    return genericInclude.Invoke(query, [include.Navigation, include.JoinKind, include.Alias])
      ?? throw new InvalidOperationException("Failed to apply include.");
  }
}

internal static class SqlWriteCompiler
{
  internal readonly record struct UpsertPlan(string Sql, IReadOnlyList<SqlPropertyMap> InsertProps);

  public static UpsertPlan BuildUpsertPlan<T>(Expression<Func<T, object>> keySelector, ISqlDialect dialect)
  {
    var map = SQLMapper.Get<T>();
    var keys = ResolveKeyProperties(keySelector, map);
    if (keys.Count == 0)
      throw new InvalidOperationException("Upsert requires at least one key column.");

    var writable = map.Writable.ToList();
    var keyNames = keys.Select(k => k.Property.Name).ToHashSet(StringComparer.Ordinal);

    var insertProps = keys
      .Concat(writable.Where(w => !keyNames.Contains(w.Property.Name)))
      .ToList();

    var updateProps = writable
      .Where(w => !keyNames.Contains(w.Property.Name))
      .ToList();

    var tableName = SqlHelper.ResolveTableName(typeof(T));
    var sql = string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
      ? BuildSqlServerUpsertSql(tableName, insertProps, keys, updateProps, dialect)
      : BuildPostgresUpsertSql(tableName, insertProps, keys, updateProps, dialect);

    return new UpsertPlan(sql, insertProps);
  }

  public static DynamicParameters BuildUpsertParameters<T>(T entity, IReadOnlyList<SqlPropertyMap> insertProps)
  {
    var parameters = new DynamicParameters();

    foreach (var prop in insertProps)
    {
      var value = prop.Property.GetValue(entity);
      if (prop.IsJson)
        parameters.Add(prop.Property.Name, JsonSerializer.Serialize(value));
      else
        parameters.Add(prop.Property.Name, value);
    }

    return parameters;
  }

  public static CompiledQuery BuildUpsert<T>(T entity, Expression<Func<T, object>> keySelector, ISqlDialect dialect)
  {
    var plan = BuildUpsertPlan(keySelector, dialect);
    var parameters = BuildUpsertParameters(entity, plan.InsertProps);
    return new CompiledQuery(plan.Sql, parameters);
  }

  private static string BuildPostgresUpsertSql(
    string tableName,
    IReadOnlyList<SqlPropertyMap> insertProps,
    IReadOnlyList<SqlPropertyMap> keys,
    IReadOnlyList<SqlPropertyMap> updateProps,
    ISqlDialect dialect)
  {
    var table = dialect.Quote(tableName);
    var columns = string.Join(", ", insertProps.Select(p => dialect.Quote(p.Column)));
    var values = string.Join(", ", insertProps.Select(p =>
      p.IsJson ? dialect.ParameterJsonCast($"@{p.Property.Name}") : $"@{p.Property.Name}"));

    var conflictCols = string.Join(", ", keys.Select(k => dialect.Quote(k.Column)));

    if (updateProps.Count == 0)
      return $"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflictCols}) DO NOTHING;";

    var updates = string.Join(", ", updateProps.Select(p =>
      $"{dialect.Quote(p.Column)} = EXCLUDED.{dialect.Quote(p.Column)}"));

    return $"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflictCols}) DO UPDATE SET {updates};";
  }

  private static string BuildSqlServerUpsertSql(
    string tableName,
    IReadOnlyList<SqlPropertyMap> insertProps,
    IReadOnlyList<SqlPropertyMap> keys,
    IReadOnlyList<SqlPropertyMap> updateProps,
    ISqlDialect dialect)
  {
    var table = dialect.Quote(tableName);
    var sourceProjection = string.Join(", ", insertProps.Select(p => $"@{p.Property.Name} AS {dialect.Quote(p.Column)}"));
    var sourceColumns = string.Join(", ", insertProps.Select(p => dialect.Quote(p.Column)));

    var onClause = string.Join(" AND ", keys.Select(k =>
      $"target.{dialect.Quote(k.Column)} = source.{dialect.Quote(k.Column)}"));

    var updateClause = updateProps.Count == 0
      ? string.Empty
      : " WHEN MATCHED THEN UPDATE SET " + string.Join(", ", updateProps.Select(p =>
        $"target.{dialect.Quote(p.Column)} = source.{dialect.Quote(p.Column)}"));

    var insertColumns = string.Join(", ", insertProps.Select(p => dialect.Quote(p.Column)));
    var insertValues = string.Join(", ", insertProps.Select(p => $"source.{dialect.Quote(p.Column)}"));

    return $"MERGE {table} AS target USING (SELECT {sourceProjection}) AS source ({sourceColumns}) ON {onClause}{updateClause} WHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({insertValues});";
  }

  private static IReadOnlyList<SqlPropertyMap> ResolveKeyProperties<T>(Expression<Func<T, object>> selector, SqlEntityMap map)
  {
    IEnumerable<string> names = selector.Body switch
    {
      MemberExpression m => [m.Member.Name],
      UnaryExpression { Operand: MemberExpression m } => [m.Member.Name],
      NewExpression n => n.Arguments
        .Select(a => a as MemberExpression ?? (a as UnaryExpression)?.Operand as MemberExpression)
        .Where(m => m != null)
        .Select(m => m!.Member.Name),
      _ => throw new NotSupportedException("Key selector must be a member access or anonymous object expression.")
    };

    var result = new List<SqlPropertyMap>();
    foreach (var name in names)
    {
      var match = map.Properties.FirstOrDefault(p => p.Property.Name == name);
      if (match == null)
        throw new InvalidOperationException($"Could not resolve key property '{name}' on {typeof(T).Name}.");

      result.Add(match);
    }

    return result;
  }
}
