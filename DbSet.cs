using System.Data;
using System.Collections.Concurrent;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
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
  IProjectedDbSet<TProjection> Select<TProjection>();
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

public interface IProjectedDbSet<TProjection>
{
  Task<List<TProjection>> ToListAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
  Task<TProjection?> FirstOrDefaultAsync(IDbTransaction? transaction = null, CancellationToken cancellationToken = default);
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

  public IProjectedDbSet<TProjection> Select<TProjection>() =>
    new ProjectedDbSet<T, TProjection>(_db, _dialect, _model);

  public CompiledQuery ToCompiledQuery() => ToCompiledQuery(SqlQueryResultMode.List);

  public CompiledQuery ToCompiledQuery(SqlQueryResultMode mode) => SqlQueryCompiler.Compile(_model, _dialect, mode);

  public async Task<List<T>> ToListAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    EnsureTransactionConnectionMatches(transaction);

    if (_model.Includes.Count > 0)
      return await ToListWithIncludesAsync(transaction, cancellationToken);

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
    EnsureTransactionConnectionMatches(transaction);

    if (_model.Includes.Count > 0)
      return await FirstOrDefaultWithIncludesAsync(transaction, cancellationToken);

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
    EnsureTransactionConnectionMatches(transaction);

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
    EnsureTransactionConnectionMatches(transaction);

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
    EnsureTransactionConnectionMatches(transaction);

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
    EnsureTransactionConnectionMatches(transaction);

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

  private void EnsureTransactionConnectionMatches(IDbTransaction? transaction)
  {
    if (transaction?.Connection != null && !ReferenceEquals(transaction.Connection, _db))
      throw new InvalidOperationException("The provided transaction does not belong to the DbSet connection.");
  }

  private async Task<List<T>> ToListWithIncludesAsync(
    IDbTransaction? transaction,
    CancellationToken cancellationToken)
  {
    var compiled = SqlQueryCompiler.CompileNested(_model, _dialect, SqlQueryResultMode.List);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    var rows = await _db.QueryAsync(command);
    return rows
      .Cast<object>()
      .Select(row => SqlQueryCompiler.MapNestedRow<T>(row, compiled))
      .ToList();
  }

  private async Task<T?> FirstOrDefaultWithIncludesAsync(
    IDbTransaction? transaction,
    CancellationToken cancellationToken)
  {
    var compiled = SqlQueryCompiler.CompileNested(_model, _dialect, SqlQueryResultMode.First);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    var row = await _db.QueryFirstOrDefaultAsync(command);
    return row == null ? default : SqlQueryCompiler.MapNestedRow<T>(row, compiled);
  }
}

public sealed class ProjectedDbSet<TRoot, TProjection> : IProjectedDbSet<TProjection>
{
  private readonly IDbConnection _db;
  private readonly ISqlDialect _dialect;
  private readonly SqlQueryModel _model;

  internal ProjectedDbSet(IDbConnection db, ISqlDialect dialect, SqlQueryModel model)
  {
    _db = db;
    _dialect = dialect;
    _model = model;
  }

  public CompiledQuery ToCompiledQuery() => ToCompiledQuery(SqlQueryResultMode.List);

  public CompiledQuery ToCompiledQuery(SqlQueryResultMode mode) =>
    SqlQueryCompiler.CompileProjected<TRoot, TProjection>(_model, _dialect, mode);

  public async Task<List<TProjection>> ToListAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    EnsureTransactionConnectionMatches(transaction);

    var compiled = ToCompiledQuery(SqlQueryResultMode.List);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    var rows = await _db.QueryAsync<TProjection>(command);
    return rows.ToList();
  }

  public async Task<TProjection?> FirstOrDefaultAsync(
    IDbTransaction? transaction = null,
    CancellationToken cancellationToken = default)
  {
    EnsureTransactionConnectionMatches(transaction);

    var compiled = ToCompiledQuery(SqlQueryResultMode.First);
    var command = new CommandDefinition(
      compiled.Sql,
      compiled.Parameters,
      transaction: transaction,
      cancellationToken: cancellationToken);

    return await _db.QueryFirstOrDefaultAsync<TProjection>(command);
  }

  private void EnsureTransactionConnectionMatches(IDbTransaction? transaction)
  {
    if (transaction?.Connection != null && !ReferenceEquals(transaction.Connection, _db))
      throw new InvalidOperationException("The provided transaction does not belong to the DbSet connection.");
  }
}

public readonly record struct CompiledQuery(string Sql, DynamicParameters Parameters);

internal readonly record struct NestedIncludePlan(string Alias, PropertyInfo NavigationProperty, SqlEntityMap Map);

internal readonly record struct NestedCompiledQuery(
  string Sql,
  DynamicParameters Parameters,
  SqlEntityMap RootMap,
  IReadOnlyList<NestedIncludePlan> Includes);

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
  private static readonly ConcurrentDictionary<string, string> ProjectionSelectCache = new();

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

    var hasPaging = model.Limit.HasValue || model.Offset.HasValue;
    var sqlServerPagingNeedsOrderFallback =
      hasPaging
      && orderParts.Count == 0
      && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase);

    var pagingSql = SqlHelper.BuildPaging(
      model.Limit,
      model.Offset,
      dialect,
      forceOrderByForSqlServer: sqlServerPagingNeedsOrderFallback);

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

  public static CompiledQuery CompileProjected<TRoot, TProjection>(
    SqlQueryModel model,
    ISqlDialect dialect,
    SqlQueryResultMode mode = SqlQueryResultMode.List)
  {
    if (model.RootType != typeof(TRoot))
      throw new InvalidOperationException("Projection root type mismatch.");

    if (mode is not (SqlQueryResultMode.List or SqlQueryResultMode.First))
      throw new NotSupportedException("Projected queries currently support only List and First modes.");

    var parameters = new DynamicParameters();
    var query = CreateTypedQuery(model.RootType, dialect);
    foreach (var include in model.Includes)
      query = ApplyInclude(query, include);

    var fromSql = (string)(query.GetType()
      .GetMethod(nameof(SqlQuery<object>.BuildFrom))
      ?.Invoke(query, null)
      ?? throw new InvalidOperationException("Failed to build projected FROM SQL."));

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

    var hasPaging = model.Limit.HasValue || model.Offset.HasValue;
    var sqlServerPagingNeedsOrderFallback =
      hasPaging
      && orderParts.Count == 0
      && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase);

    var pagingSql = SqlHelper.BuildPaging(
      model.Limit,
      model.Offset,
      dialect,
      forceOrderByForSqlServer: sqlServerPagingNeedsOrderFallback);

    var paging = string.IsNullOrWhiteSpace(pagingSql) ? string.Empty : " " + pagingSql;

    var selectSql = BuildProjectionSelectSql(model.RootType, model.Includes, typeof(TProjection), dialect, "t0");
    var baseQuerySql = $"{selectSql} {fromSql}{whereSql}";

    var sql = mode switch
    {
      SqlQueryResultMode.List => $"{baseQuerySql}{orderSql}{paging}",
      SqlQueryResultMode.First => BuildFirstSql(baseQuerySql, orderSql, dialect),
      _ => throw new NotSupportedException("Unsupported projection query mode")
    };

    return new CompiledQuery(sql, parameters);
  }

  public static NestedCompiledQuery CompileNested(
    SqlQueryModel model,
    ISqlDialect dialect,
    SqlQueryResultMode mode)
  {
    if (mode is not (SqlQueryResultMode.List or SqlQueryResultMode.First))
      throw new NotSupportedException("Nested query compilation supports List and First modes only.");

    var parameters = new DynamicParameters();
    var query = CreateTypedQuery(model.RootType, dialect);
    foreach (var include in model.Includes)
      query = ApplyInclude(query, include);

    var fromSql = (string)(query.GetType()
      .GetMethod(nameof(SqlQuery<object>.BuildFrom))
      ?.Invoke(query, null)
      ?? throw new InvalidOperationException("Failed to build nested FROM SQL."));

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

    var hasPaging = model.Limit.HasValue || model.Offset.HasValue;
    var sqlServerPagingNeedsOrderFallback =
      hasPaging
      && orderParts.Count == 0
      && string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase);

    var pagingSql = SqlHelper.BuildPaging(
      model.Limit,
      model.Offset,
      dialect,
      forceOrderByForSqlServer: sqlServerPagingNeedsOrderFallback);

    var paging = string.IsNullOrWhiteSpace(pagingSql) ? string.Empty : " " + pagingSql;

    var rootMap = SQLMapper.Get(model.RootType);
    var includePlans = BuildNestedIncludePlans(model.RootType, model.Includes);
    var selectSql = BuildNestedSelectSql(rootMap, includePlans, dialect);

    var baseSql = $"{selectSql} {fromSql}{whereSql}";
    var sql = mode switch
    {
      SqlQueryResultMode.List => $"{baseSql}{orderSql}{paging}",
      SqlQueryResultMode.First => BuildFirstSql(baseSql, orderSql, dialect),
      _ => throw new NotSupportedException($"Unsupported nested mode: {mode}")
    };

    return new NestedCompiledQuery(sql, parameters, rootMap, includePlans);
  }

  public static T MapNestedRow<T>(object row, NestedCompiledQuery compiled)
  {
    var values = row switch
    {
      IDictionary<string, object> typed => typed.ToDictionary(k => k.Key, v => (object?)v.Value, StringComparer.OrdinalIgnoreCase),
      IDictionary nonGeneric => nonGeneric.Keys.Cast<object>()
        .ToDictionary(k => k.ToString() ?? string.Empty, k => nonGeneric[k], StringComparer.OrdinalIgnoreCase),
      _ => throw new InvalidOperationException("Nested mapping expects dictionary-backed Dapper rows.")
    };
    var root = Activator.CreateInstance<T>()
      ?? throw new InvalidOperationException($"Could not create instance of {typeof(T).Name}.");

    foreach (var prop in compiled.RootMap.Selectable)
    {
      var key = $"root__{prop.Property.Name}";
      if (values.TryGetValue(key, out var value))
        AssignPropertyValue(root, prop.Property, value);
    }

    foreach (var include in compiled.Includes)
    {
      var includeInstance = Activator.CreateInstance(include.Map.EntityType);
      if (includeInstance == null)
        continue;

      var hasAnyValue = false;
      foreach (var includeProp in include.Map.Selectable)
      {
        var key = $"inc__{include.Alias}__{includeProp.Property.Name}";
        if (!values.TryGetValue(key, out var value))
          continue;

        if (value is not null and not DBNull)
          hasAnyValue = true;

        AssignPropertyValue(includeInstance, includeProp.Property, value);
      }

      if (hasAnyValue)
        include.NavigationProperty.SetValue(root, includeInstance);
    }

    return root;
  }

  private static string BuildProjectionSelectSql(
    Type rootType,
    IReadOnlyList<IncludeSpec> includes,
    Type projectionType,
    ISqlDialect dialect,
    string alias)
  {
    var includeSignature = includes.Count == 0
      ? "none"
      : string.Join(";", includes.Select(i =>
      {
        var navName = ExtractPropertyInfo(i.Navigation).Name;
        return $"{i.JoinType.FullName}:{i.JoinKind}:{i.Alias ?? "(auto)"}:{navName}";
      }));

    var cacheKey = $"{dialect.Name}|{rootType.FullName}|{projectionType.FullName}|{alias}|{includeSignature}";

    return ProjectionSelectCache.GetOrAdd(cacheKey, _ =>
    {
      var projectionMap = SQLMapper.Get(projectionType);
      var sources = BuildProjectionSources(rootType, includes, alias);

      var selectColumns = new List<string>();
      foreach (var projectionProp in projectionMap.Selectable)
      {
        var matches = sources
          .SelectMany(source => source.Map.Selectable
            .Where(prop => prop.Column.Equals(projectionProp.Column, StringComparison.OrdinalIgnoreCase))
            .Select(prop => (source, prop)))
          .ToList();

        if (matches.Count == 0)
          throw new InvalidOperationException(
            $"Projection property '{projectionProp.Property.Name}' ({projectionProp.Column}) does not map to any selectable source in query root/includes.");

        var selected = ResolveProjectionMatch(matches, projectionProp, rootType);

        selectColumns.Add($"{selected.source.Alias}.{dialect.Quote(selected.prop.Column)} AS {dialect.Quote(projectionProp.Property.Name)}");
      }

      if (selectColumns.Count == 0)
        throw new InvalidOperationException($"Projection type '{projectionType.Name}' has no selectable properties.");

      return "SELECT " + string.Join(", ", selectColumns);
    });
  }

  private static IReadOnlyList<ProjectionSource> BuildProjectionSources(
    Type rootType,
    IReadOnlyList<IncludeSpec> includes,
    string rootAlias)
  {
    var sources = new List<ProjectionSource>
    {
      new(rootAlias, rootType, SQLMapper.Get(rootType))
    };

    for (var includeIndex = 0; includeIndex < includes.Count; includeIndex++)
    {
      var include = includes[includeIndex];
      var navProp = ExtractPropertyInfo(include.Navigation);
      var relation = navProp.GetCustomAttribute<DbRelationAttribute>()
        ?? throw new InvalidOperationException($"Property {navProp.Name} is missing [DbRelation].");

      if (relation.RelatedType != include.JoinType)
        throw new InvalidOperationException($"[DbRelation] type mismatch on {navProp.Name}. Expected {include.JoinType.Name}.");

      var resolvedAlias = include.Alias ?? relation.Alias ?? $"t{includeIndex + 1}";
      if (sources.Any(s => string.Equals(s.Alias, resolvedAlias, StringComparison.OrdinalIgnoreCase)))
        throw new InvalidOperationException($"Duplicate include alias '{resolvedAlias}' detected.");

      sources.Add(new ProjectionSource(
        resolvedAlias,
        include.JoinType,
        SQLMapper.Get(include.JoinType)));
    }

    return sources;
  }

  private static PropertyInfo ExtractPropertyInfo(LambdaExpression expression)
  {
    return expression.Body switch
    {
      MemberExpression m when m.Member is PropertyInfo p => p,
      UnaryExpression { Operand: MemberExpression m } when m.Member is PropertyInfo p => p,
      _ => throw new InvalidOperationException("Navigation expression must target a property.")
    };
  }

  private static IReadOnlyList<NestedIncludePlan> BuildNestedIncludePlans(
    Type rootType,
    IReadOnlyList<IncludeSpec> includes)
  {
    var plans = new List<NestedIncludePlan>();

    for (var includeIndex = 0; includeIndex < includes.Count; includeIndex++)
    {
      var include = includes[includeIndex];
      var navProp = ExtractPropertyInfo(include.Navigation);
      var relation = navProp.GetCustomAttribute<DbRelationAttribute>()
        ?? throw new InvalidOperationException($"Property {navProp.Name} is missing [DbRelation].");

      if (relation.RelatedType != include.JoinType)
        throw new InvalidOperationException($"[DbRelation] type mismatch on {navProp.Name}. Expected {include.JoinType.Name}.");

      var resolvedAlias = include.Alias ?? relation.Alias ?? $"t{includeIndex + 1}";
      if (plans.Any(p => string.Equals(p.Alias, resolvedAlias, StringComparison.OrdinalIgnoreCase)))
        throw new InvalidOperationException($"Duplicate include alias '{resolvedAlias}' detected.");

      plans.Add(new NestedIncludePlan(
        resolvedAlias,
        navProp,
        SQLMapper.Get(include.JoinType)));
    }

    return plans;
  }

  private static string BuildNestedSelectSql(
    SqlEntityMap rootMap,
    IReadOnlyList<NestedIncludePlan> includes,
    ISqlDialect dialect)
  {
    var cols = new List<string>();

    foreach (var rootProp in rootMap.Selectable)
      cols.Add($"t0.{dialect.Quote(rootProp.Column)} AS {dialect.Quote($"root__{rootProp.Property.Name}")}");

    foreach (var include in includes)
    {
      foreach (var includeProp in include.Map.Selectable)
        cols.Add($"{include.Alias}.{dialect.Quote(includeProp.Column)} AS {dialect.Quote($"inc__{include.Alias}__{includeProp.Property.Name}")}");
    }

    return "SELECT " + string.Join(", ", cols);
  }

  private static void AssignPropertyValue(object target, PropertyInfo property, object? rawValue)
  {
    if (rawValue is null or DBNull)
      return;

    var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
    var sourceType = rawValue.GetType();

    object converted = targetType.IsAssignableFrom(sourceType)
      ? rawValue
      : Convert.ChangeType(rawValue, targetType);

    property.SetValue(target, converted);
  }

  private readonly record struct ProjectionSource(string Alias, Type ModelType, SqlEntityMap Map);

  private static (ProjectionSource source, SqlPropertyMap prop) ResolveProjectionMatch(
    IReadOnlyList<(ProjectionSource source, SqlPropertyMap prop)> matches,
    SqlPropertyMap projectionProp,
    Type rootType)
  {
    var hint = projectionProp.Property.GetCustomAttribute<ProjectionSourceAttribute>();
    if (hint != null)
    {
      var hintedMatches = matches.Where(m =>
      {
        var aliasMatch = !string.IsNullOrWhiteSpace(hint.Alias)
          && string.Equals(m.source.Alias, hint.Alias, StringComparison.OrdinalIgnoreCase);

        var typeMatch = hint.ModelType != null && m.source.ModelType == hint.ModelType;
        return aliasMatch || typeMatch;
      }).ToList();

      if (hintedMatches.Count == 1)
        return hintedMatches[0];

      if (hintedMatches.Count == 0)
        throw new InvalidOperationException(
          $"Projection property '{projectionProp.Property.Name}' specifies [ProjectionSource] but no matching source was found.");

      throw new InvalidOperationException(
        $"Projection property '{projectionProp.Property.Name}' [ProjectionSource] matches multiple sources.");
    }

    var rootMatches = matches.Where(m => m.source.ModelType == rootType).ToList();
    if (rootMatches.Count == 1)
      return rootMatches[0];

    if (matches.Count == 1)
      return matches[0];

    throw new InvalidOperationException(
      $"Projection property '{projectionProp.Property.Name}' ({projectionProp.Column}) is ambiguous. Add [ProjectionSource(typeof(...))] or [ProjectionSource(\"alias\")].");
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

    try
    {
      return genericInclude.Invoke(query, [include.Navigation, include.JoinKind, include.Alias])
        ?? throw new InvalidOperationException("Failed to apply include.");
    }
    catch (TargetInvocationException ex) when (ex.InnerException != null)
    {
      ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
      throw;
    }
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
