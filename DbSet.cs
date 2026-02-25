using System.Data;
using System.Linq.Expressions;
using System.Reflection;
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
  CompiledQuery ToCompiledQuery();
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

  public CompiledQuery ToCompiledQuery() => SqlQueryCompiler.Compile(_model, _dialect);

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
}

public readonly record struct CompiledQuery(string Sql, DynamicParameters Parameters);

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
  public static CompiledQuery Compile(SqlQueryModel model, ISqlDialect dialect)
  {
    var parameters = new DynamicParameters();
    var query = CreateTypedQuery(model.RootType, dialect);

    foreach (var include in model.Includes)
      query = ApplyInclude(query, include);

    var selectFrom = (string)(query.GetType()
      .GetMethod(nameof(SqlQuery<object>.BuildSelectFrom))
      ?.Invoke(query, null)
      ?? throw new InvalidOperationException("Failed to build SELECT/FROM SQL."));

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

    var sql = $"{selectFrom}{whereSql}{orderSql}{paging}";
    return new CompiledQuery(sql, parameters);
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
