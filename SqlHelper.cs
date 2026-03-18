using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Dapper;

namespace CodeWorks.SimpleSql;

#region Entity Mapping

public sealed class SqlEntityMap
{
  public Type EntityType { get; }
  public IReadOnlyList<SqlPropertyMap> Properties { get; }

  public IReadOnlyList<SqlPropertyMap> Writable =>
    Properties.Where(p => p.Writable).ToList();

  public IReadOnlyList<SqlPropertyMap> Selectable =>
    Properties.Where(p => p.Selectable).ToList();

  internal SqlEntityMap(Type type)
  {
    EntityType = type;

    Properties = type
      .GetProperties(BindingFlags.Public | BindingFlags.Instance)
      .Select(p => new SqlPropertyMap(p))
      .ToList();
  }
}

public sealed class SqlPropertyMap
{
  public PropertyInfo Property { get; }
  public string Column { get; }
  public bool Writable { get; }
  public bool Selectable { get; }
  public bool IsJson { get; }

  public SqlPropertyMap(PropertyInfo prop)
  {
    Property = prop;

    var db = prop.GetCustomAttribute<DbColumnAttribute>();
    Column = db?.ColumnName ?? SqlFilterBuilder.ToSnakeCase(prop.Name);

    Writable =
      prop.CanWrite &&
      prop.GetCustomAttribute<IgnoreWriteAttribute>() == null &&
      db?.Writable != false &&
      prop.Name is not ("Id" or "CreatedAt" or "UpdatedAt");

    Selectable =
      prop.GetCustomAttribute<IgnoreSelectAttribute>() == null &&
      db?.Selectable != false;

    IsJson =
      prop.GetCustomAttribute<JsonColumnAttribute>() != null;
  }
}

public static class SQLMapper
{
  private static readonly ConcurrentDictionary<Type, SqlEntityMap> Cache = new();

  public static SqlEntityMap Get<T>() =>
    Cache.GetOrAdd(typeof(T), t => new SqlEntityMap(t));

  public static SqlEntityMap Get(Type t) =>
    Cache.GetOrAdd(t, x => new SqlEntityMap(x));
}

#endregion

#region SQL Generation Helpers

public sealed class EntityToSql
{
  public SqlEntityMap Map { get; }
  public ISqlDialect Dialect { get; }

  public EntityToSql(SqlEntityMap map, ISqlDialect? dialect = null)
  {
    Map = map;
    Dialect = dialect ?? SqlHelper.Dialect;
  }

  public string InsertColumns =>
    string.Join(", ", Map.Writable.Select(p => Dialect.Quote(p.Column)));

  public string InsertValues =>
    string.Join(", ", Map.Writable.Select(p =>
      p.IsJson ? Dialect.ParameterJsonCast($"@{p.Property.Name}") : $"@{p.Property.Name}"
    ));

  public string UpdateSet =>
    string.Join(", ", Map.Writable.Select(p =>
      p.IsJson
        ? $"{Dialect.Quote(p.Column)} = {Dialect.ParameterJsonCast($"@{p.Property.Name}")}"
        : $"{Dialect.Quote(p.Column)} = @{p.Property.Name}"
    ));

  public string SelectColumns(string? alias = null) =>
    string.Join(", ", Map.Selectable.Select(p =>
      alias != null
        ? $"{alias}.{Dialect.Quote(p.Column)} AS {Dialect.Quote(p.Property.Name)}"
        : $"{Dialect.Quote(p.Column)} AS {Dialect.Quote(p.Property.Name)}"
    ));
}

#endregion

#region Shared Predicate Context

internal sealed class SqlPredicateContext
{
  public SqlEntityMap Map { get; }
  public DynamicParameters Parameters { get; }
  public string? Alias { get; }
  public ISqlDialect Dialect { get; }

  private int _paramIndex = 0;

  public SqlPredicateContext(
    SqlEntityMap map,
    DynamicParameters parameters,
    string? alias,
    ISqlDialect dialect
  )
  {
    Map = map;
    Parameters = parameters;
    Alias = alias;
    Dialect = dialect;
  }

  public string Column(PropertyInfo prop)
  {
    var db = prop.GetCustomAttribute<DbColumnAttribute>();
    var column = Dialect.Quote(db?.ColumnName ?? SqlFilterBuilder.ToSnakeCase(prop.Name));
    return Alias != null ? $"{Alias}.{column}" : column;
  }

  public string AddParam(object? value)
  {
    var name = $"@p{_paramIndex++}";
    Parameters.Add(name, Normalize(value));
    return name;
  }

  private static object? Normalize(object? value)
  {
    if (value is DateTime dt)
      return dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();

    return value;
  }
}

#endregion

#region Filter Builder

public static class SqlFilterBuilder
{
  public static (string whereSql, string? rankSql) Build(
    string[] searchableFields,
    string? keyword,
    Dictionary<string, string>? filters,
    DynamicParameters parameters,
    SqlEntityMap? entity = null,
    bool hasSearchVector = false,
    string searchVectorColumn = "search_vector",
    ISqlDialect? dialect = null
  )
  {
    var activeDialect = dialect ?? SqlHelper.Dialect;
    var clauses = new List<string>();
    string? rankSql = null;

    if (!string.IsNullOrWhiteSpace(keyword))
    {
      var whereSql = BuildKeyword(
        keyword, searchableFields, parameters,
        hasSearchVector, searchVectorColumn, out rankSql, activeDialect
      );
      clauses.Add(whereSql);
    }

    if (filters != null && entity != null)
      clauses.Add(BuildFilters(filters, parameters, entity, activeDialect));

    var valid = clauses.Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

    return (
      valid.Count > 0 ? "WHERE " + string.Join(" AND ", valid) : "",
      rankSql
    );
  }

  private static string BuildFilters(
    Dictionary<string, string> filters,
    DynamicParameters parameters,
    SqlEntityMap entity,
    ISqlDialect dialect
)
  {
    var ctx = new SqlPredicateContext(entity, parameters, null, dialect);
    var clauses = new List<string>();

    foreach (var (key, raw) in filters)
    {
      var prop = entity.Properties
          .FirstOrDefault(p => p.Property.Name.Equals(ToPascalCase(key), StringComparison.OrdinalIgnoreCase))
          ?.Property;

      if (prop == null)
        continue;

      var underlyingType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
      var column = ctx.Column(prop);

      if (raw.Contains(','))
      {
        var values = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var paramNames = values.Select(v =>
        {
          object typedValue = ParseValue(v, underlyingType);
          return ctx.AddParam(typedValue);
        }).ToList();

        clauses.Add($"{column} IN ({string.Join(", ", paramNames)})");
        continue;
      }

      if (underlyingType == typeof(string))
      {
        var param = ctx.AddParam($"%{raw}%");
        clauses.Add($"LOWER({dialect.CastToText(column)}) LIKE LOWER({param})");
        continue;
      }

      var typedSingleValue = ParseValue(raw, underlyingType);
      var paramName = ctx.AddParam(typedSingleValue);
      clauses.Add($"{column} = {paramName}");
    }

    return clauses.Count > 0
        ? "(" + string.Join(" AND ", clauses) + ")"
        : "";
  }

  private static object ParseValue(string raw, Type targetType)
  {
    if (targetType == typeof(Guid)) return Guid.Parse(raw);
    if (targetType == typeof(DateTime)) return DateTime.Parse(raw);
    if (targetType == typeof(bool)) return bool.Parse(raw);
    if (targetType.IsEnum) return Enum.Parse(targetType, raw, ignoreCase: true);

    return Convert.ChangeType(raw, targetType);
  }

  private static string BuildKeyword(
    string keyword,
    string[] searchableFields,
    DynamicParameters parameters,
    bool hasSearchVector,
    string searchVectorColumn,
    out string? rankSql,
    ISqlDialect dialect,
    bool enableFuzzy = true,
    bool enablePrefix = true,
    bool looseTokenMatch = false
)
  {
    rankSql = null;

    // --- PostgreSQL full‑text search with weighting ---
    if (hasSearchVector && string.Equals(dialect.Name, "postgres", StringComparison.OrdinalIgnoreCase))
    {
      parameters.Add("q", keyword);

      var tsQuerySql = """
            (
              SELECT to_tsquery(
                'simple',
                string_agg(term || ':*', ' & ')
              )
              FROM unnest(regexp_split_to_array(@q, '\s+')) term
            )
        """;

      // Weighted rank (A highest, D lowest)
      rankSql = $"ts_rank({searchVectorColumn}, {tsQuerySql}, 1)";

      return $"{searchVectorColumn} @@ {tsQuerySql}";
    }

    // --- Multi‑term LIKE search ---
    var tokens = keyword
        .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(t => t.ToLower())
        .ToArray();

    var tokenClauses = new List<string>();

    for (int i = 0; i < tokens.Length; i++)
    {
      var token = tokens[i];
      var paramLike = $"q_like_{i}";
      var paramPrefix = $"q_prefix_{i}";
      var paramFuzzy = $"q_fuzzy_{i}";

      parameters.Add(paramLike, $"%{token}%");
      parameters.Add(paramPrefix, $"{token}%");
      parameters.Add(paramFuzzy, token);

      var fieldMatches = new List<string>();

      foreach (var f in searchableFields)
      {
        var col = $"LOWER({dialect.CastToText(dialect.Quote(f))})";

        // Contains
        fieldMatches.Add($"{col} LIKE LOWER(@{paramLike})");

        // Prefix
        if (enablePrefix)
          fieldMatches.Add($"{col} LIKE LOWER(@{paramPrefix})");

        // Fuzzy (Postgres only)
        if (enableFuzzy && dialect.Name.Equals("postgres", StringComparison.OrdinalIgnoreCase))
          fieldMatches.Add($"levenshtein({col}, @{paramFuzzy}) <= 2");
      }

      // OR across fields
      var orGroup = "(" + string.Join(" OR ", fieldMatches) + ")";

      tokenClauses.Add(orGroup);
    }

    // AND across tokens (default) or OR (loose mode)
    return looseTokenMatch
        ? "(" + string.Join(" OR ", tokenClauses) + ")"
        : "(" + string.Join(" AND ", tokenClauses) + ")";
  }




  public static string ToSnakeCase(string input)
  {
    if (string.IsNullOrEmpty(input)) return input;

    var sb = new System.Text.StringBuilder();
    for (int i = 0; i < input.Length; i++)
    {
      var c = input[i];
      if (char.IsUpper(c))
      {
        if (i > 0) sb.Append('_');
        sb.Append(char.ToLower(c));
      }
      else sb.Append(c);
    }
    return sb.ToString();
  }

  public static string ToPascalCase(string input)
  {
    if (string.IsNullOrEmpty(input)) return input;
    if (char.IsLower(input[0]) && input.Skip(1).Any(char.IsUpper))
      input = char.ToUpper(input[0]) + input[1..];

    var words = input.Split(['_', ' '], StringSplitOptions.RemoveEmptyEntries);
    return string.Concat(words.Select(w => char.ToUpper(w[0]) + w[1..].ToLower()));
  }
}

#endregion

#region Expression Builder

public static class SqlExpressionBuilder
{
  public static string Build(
    LambdaExpression predicate,
    SqlEntityMap map,
    DynamicParameters parameters,
    string? alias = null,
    ISqlDialect? dialect = null
  )
  {
    if (predicate.Parameters.Count != 1 || predicate.Parameters[0].Type != map.EntityType)
      throw new InvalidOperationException($"Predicate parameter type must match {map.EntityType.Name}.");

    var activeDialect = dialect ?? SqlHelper.Dialect;
    var ctx = new SqlPredicateContext(map, parameters, alias, activeDialect);
    var visitor = new SqlExpressionVisitor(ctx);

    visitor.Visit(predicate.Body);

    return $"WHERE {visitor.Sql}";
  }

  public static string Build<T>(
    Expression<Func<T, bool>> predicate,
    SqlEntityMap map,
    DynamicParameters parameters,
    string? alias = null,
    ISqlDialect? dialect = null
  )
  {
    var activeDialect = dialect ?? SqlHelper.Dialect;
    var ctx = new SqlPredicateContext(map, parameters, alias, activeDialect);
    var visitor = new SqlExpressionVisitor(ctx);

    visitor.Visit(predicate.Body);

    return $"WHERE {visitor.Sql}";
  }

  public static (string whereSql, DynamicParameters parameters) Build<T>(
    Expression<Func<T, bool>> predicate,
    string? alias = null,
    ISqlDialect? dialect = null
  )
  {
    var map = SQLMapper.Get<T>();
    var parameters = new DynamicParameters();
    var activeDialect = dialect ?? SqlHelper.Dialect;
    var ctx = new SqlPredicateContext(map, parameters, alias, activeDialect);
    var visitor = new SqlExpressionVisitor(ctx);

    visitor.Visit(predicate.Body);

    return ($"WHERE {visitor.Sql}", parameters);
  }
}

internal sealed class SqlExpressionVisitor(SqlPredicateContext ctx) : ExpressionVisitor
{
  private readonly SqlPredicateContext _ctx = ctx;
  public string Sql { get; private set; } = "";

  protected override Expression VisitMethodCall(MethodCallExpression node)
  {
    if (node.Method.Name == "Contains" &&
        node.Object == null &&
        node.Arguments.Count == 2)
    {
      var values =
        Expression.Lambda(node.Arguments[0])
          .Compile()
          .DynamicInvoke() as IEnumerable
          ?? throw new Exception("Invalid IN expression");

      Visit(node.Arguments[1]);
      var column = Sql;

      var paramList = new List<string>();
      foreach (var v in values)
        paramList.Add(_ctx.AddParam(v));

      Sql = $"{column} IN ({string.Join(", ", paramList)})";
      return node;
    }

    if (node.Method.DeclaringType == typeof(string))
    {
      Visit(node.Object!);
      var column = Sql;
      var value = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke()?.ToString() ?? "";

      var like = node.Method.Name switch
      {
        "StartsWith" => $"{value}%",
        "EndsWith" => $"%{value}",
        "Contains" => $"%{value}%",
        _ => throw new NotSupportedException($"Method {node.Method.Name} is not supported")
      };

      var param = _ctx.AddParam(like);
      Sql = $"LOWER({_ctx.Dialect.CastToText(column)}) LIKE LOWER({param})";
      return node;
    }

    return base.VisitMethodCall(node);
  }

  protected override Expression VisitBinary(BinaryExpression node)
  {
    Visit(node.Left);
    var left = Sql;

    Visit(node.Right);
    var right = Sql;

    if (right == "NULL")
    {
      Sql = node.NodeType switch
      {
        ExpressionType.Equal => $"{left} IS NULL",
        ExpressionType.NotEqual => $"{left} IS NOT NULL",
        _ => throw new NotSupportedException()
      };
      return node;
    }

    var op = node.NodeType switch
    {
      ExpressionType.Equal => "=",
      ExpressionType.NotEqual => "!=",
      ExpressionType.AndAlso => "AND",
      ExpressionType.OrElse => "OR",
      ExpressionType.GreaterThan => ">",
      ExpressionType.GreaterThanOrEqual => ">=",
      ExpressionType.LessThan => "<",
      ExpressionType.LessThanOrEqual => "<=",
      _ => throw new NotSupportedException()
    };

    Sql = $"({left} {op} {right})";
    return node;
  }

  protected override Expression VisitMember(MemberExpression node)
  {
    if (node.Expression is ParameterExpression)
    {
      var prop = _ctx.Map.Properties.First(p => p.Property.Name == node.Member.Name).Property;
      Sql = _ctx.Column(prop);
      return node;
    }

    Sql = _ctx.AddParam(Expression.Lambda(node).Compile().DynamicInvoke());
    return node;
  }

  protected override Expression VisitConstant(ConstantExpression node)
  {
    Sql = node.Value == null ? "NULL" : _ctx.AddParam(node.Value);
    return node;
  }
}

#endregion

#region Include / Join Query Builder

public enum SqlJoinType
{
  Inner,
  Left,
  Right
}

public sealed class SqlQuery<T>
{
  private readonly List<string> _joinSql = [];
  private readonly List<string> _selectSql = [];
  private readonly Dictionary<Type, string> _aliases = new();

  public ISqlDialect Dialect { get; }
  public string RootAlias { get; }

  public SqlQuery(string rootAlias = "t0", ISqlDialect? dialect = null)
  {
    Dialect = dialect ?? SqlHelper.Dialect;
    RootAlias = rootAlias;

    _aliases[typeof(T)] = rootAlias;
    _selectSql.Add(SqlHelper.For<T>(Dialect).SelectColumns(rootAlias));
  }

  public SqlQuery<T> Include<TJoin>(
    Expression<Func<T, object?>> navigation,
    SqlJoinType joinType = SqlJoinType.Left,
    string? alias = null)
  {
    var navProp = ExtractPropertyInfo(navigation);
    var rel = navProp.GetCustomAttribute<DbRelationAttribute>()
      ?? throw new InvalidOperationException($"Property {navProp.Name} is missing [DbRelation].");

    if (rel.RelatedType != typeof(TJoin))
      throw new InvalidOperationException($"[DbRelation] type mismatch on {navProp.Name}. Expected {typeof(TJoin).Name}.");

    var parentMap = SQLMapper.Get<T>();
    var childMap = SQLMapper.Get<TJoin>();
    var parentAlias = _aliases[typeof(T)];
    var childAlias = alias ?? rel.Alias ?? $"t{_aliases.Count}";

    var parentKey = parentMap.Properties.FirstOrDefault(p => p.Property.Name == rel.LocalKey)
      ?? throw new InvalidOperationException($"Could not find local key {rel.LocalKey} on {typeof(T).Name}.");

    var childKey = childMap.Properties.FirstOrDefault(p => p.Property.Name == rel.ForeignKey)
      ?? throw new InvalidOperationException($"Could not find foreign key {rel.ForeignKey} on {typeof(TJoin).Name}.");

    var joinKeyword = joinType switch
    {
      SqlJoinType.Inner => "INNER JOIN",
      SqlJoinType.Right => "RIGHT JOIN",
      _ => "LEFT JOIN"
    };

    var table = SqlHelper.ResolveTableName(typeof(TJoin));
    _joinSql.Add($"{joinKeyword} {Dialect.Quote(table)} {childAlias} ON {parentAlias}.{Dialect.Quote(parentKey.Column)} = {childAlias}.{Dialect.Quote(childKey.Column)}");
    _selectSql.Add(SqlHelper.For<TJoin>(Dialect).SelectColumns(childAlias));
    _aliases[typeof(TJoin)] = childAlias;

    return this;
  }

  public string BuildFrom()
  {
    var rootTable = SqlHelper.ResolveTableName(typeof(T));
    var joins = _joinSql.Count == 0 ? string.Empty : " " + string.Join(" ", _joinSql);
    return $"FROM {Dialect.Quote(rootTable)} {RootAlias}{joins}";
  }

  public string BuildSelect() => "SELECT " + string.Join(", ", _selectSql);

  public string BuildSelectFrom() => $"{BuildSelect()} {BuildFrom()}";

  private static PropertyInfo ExtractPropertyInfo(Expression<Func<T, object?>> expression)
  {
    return expression.Body switch
    {
      MemberExpression m when m.Member is PropertyInfo p => p,
      UnaryExpression { Operand: MemberExpression m } when m.Member is PropertyInfo p => p,
      _ => throw new InvalidOperationException("Navigation expression must target a property.")
    };
  }
}

#endregion

#region Public API

public static class SqlHelper
{
  public static ISqlDialect Dialect { get; private set; } = SqlDialects.Postgres;

  public static void UseDialect(ISqlDialect dialect)
  {
    Dialect = dialect;
  }

  public static EntityToSql For<T>(ISqlDialect? dialect = null) => new(SQLMapper.Get<T>(), dialect ?? Dialect);

  public static SqlQuery<T> Query<T>(string rootAlias = "t0", ISqlDialect? dialect = null) =>
    new(rootAlias, dialect ?? Dialect);

  public static string ResolveTableName(Type entityType)
  {
    var attr = entityType.GetCustomAttribute<DbTableAttribute>();
    return attr?.TableName ?? ToSnakeCase(entityType.Name) ?? entityType.Name;
  }

  public static (string whereSql, string? rankSql) BuildFilter<T>(
      string[] searchableFields,
      string? keyword,
      Dictionary<string, string>? filters,
      DynamicParameters parameters,
      bool hasSearchVector = false,
      string searchVectorColumn = "search_vector",
      ISqlDialect? dialect = null)
  {
    return SqlFilterBuilder.Build(
        searchableFields,
        keyword,
        filters,
        parameters,
        SQLMapper.Get<T>(),
        hasSearchVector,
        searchVectorColumn,
        dialect ?? Dialect
    );
  }

  public static string BuildWhere<T>(
      Expression<Func<T, bool>> predicate,
      DynamicParameters parameters,
      string? alias = null,
      ISqlDialect? dialect = null)
  {
    return SqlExpressionBuilder.Build(predicate, SQLMapper.Get<T>(), parameters, alias, dialect ?? Dialect);
  }

  public static (string whereSql, DynamicParameters parameters) BuildWhere<T>(
      Expression<Func<T, bool>> predicate,
      string? alias = null,
      ISqlDialect? dialect = null)
  {
    return SqlExpressionBuilder.Build(predicate, alias, dialect ?? Dialect);
  }

  public static string BuildPaging(int? limit, int? offset)
  {
    return BuildPaging(limit, offset, Dialect);
  }

  public static string BuildPaging(
      int? limit,
      int? offset,
      ISqlDialect? dialect,
      bool forceOrderByForSqlServer = false)
  {
    var activeDialect = dialect ?? Dialect;

    if (string.Equals(activeDialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
    {
      if (!limit.HasValue && !offset.HasValue)
        return string.Empty;

      var normalizedOffset = Math.Max(offset ?? 0, 0);
      var normalizedLimit = limit.HasValue ? Math.Clamp(limit.Value, 1, 500) : (int?)null;
      var orderBy = forceOrderByForSqlServer ? "ORDER BY (SELECT 1) " : string.Empty;

      if (normalizedLimit.HasValue)
        return $"{orderBy}OFFSET {normalizedOffset} ROWS FETCH NEXT {normalizedLimit.Value} ROWS ONLY";

      return $"{orderBy}OFFSET {normalizedOffset} ROWS";
    }

    var clauses = new List<string>();
    if (limit.HasValue) clauses.Add($"LIMIT {Math.Clamp(limit.Value, 1, 500)}");
    if (offset.HasValue) clauses.Add($"OFFSET {Math.Max(offset.Value, 0)}");
    return string.Join(" ", clauses);
  }

  public static string BuildOrderBy<T>(
      Expression<Func<T, object>> expr,
      string alias,
      bool desc = false,
      ISqlDialect? dialect = null)
  {
    var activeDialect = dialect ?? Dialect;
    var map = SQLMapper.Get<T>();
    MemberExpression member = expr.Body switch
    {
      MemberExpression m => m,
      UnaryExpression u when u.Operand is MemberExpression m => m,
      _ => throw new NotSupportedException("Invalid order expression")
    };

    var prop = map.Properties.First(p => p.Property.Name == member.Member.Name).Property;
    var column = activeDialect.Quote(prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName ?? SqlFilterBuilder.ToSnakeCase(prop.Name));
    return $"{alias}.{column} {(desc ? "DESC" : "ASC")}";
  }

  public static string BuildOrderBy(
      LambdaExpression expr,
      Type entityType,
      string alias,
      bool desc = false,
      ISqlDialect? dialect = null)
  {
    if (expr.Parameters.Count != 1 || expr.Parameters[0].Type != entityType)
      throw new InvalidOperationException($"Order expression parameter type must match {entityType.Name}.");

    var activeDialect = dialect ?? Dialect;
    var map = SQLMapper.Get(entityType);

    MemberExpression member = expr.Body switch
    {
      MemberExpression m => m,
      UnaryExpression u when u.Operand is MemberExpression m => m,
      _ => throw new NotSupportedException("Invalid order expression")
    };

    var prop = map.Properties.First(p => p.Property.Name == member.Member.Name).Property;
    var column = activeDialect.Quote(prop.GetCustomAttribute<DbColumnAttribute>()?.ColumnName ?? SqlFilterBuilder.ToSnakeCase(prop.Name));
    return $"{alias}.{column} {(desc ? "DESC" : "ASC")}";
  }

  public static DynamicParameters ToParameters<T>(T entity, int? index = null)
  {
    var map = SQLMapper.Get<T>();
    var parameters = new DynamicParameters();

    foreach (var prop in map.Writable)
    {
      var paramName = index.HasValue ? $"{prop.Property.Name}_{index}" : prop.Property.Name;
      var value = prop.Property.GetValue(entity);

      if (prop.IsJson)
        parameters.Add(paramName, JsonSerializer.Serialize(value));
      else
        parameters.Add(paramName, value);
    }

    return parameters;
  }

  public static string? ToSnakeCase(string name)
  {
    return string.IsNullOrEmpty(name) ? name : SqlFilterBuilder.ToSnakeCase(name);
  }

  public static string? ToPascalCase(string name)
  {
    return string.IsNullOrEmpty(name) ? name : SqlFilterBuilder.ToPascalCase(name);
  }
}

#endregion

#region Attributes

[AttributeUsage(AttributeTargets.Property)]
public sealed class IgnoreWriteAttribute : Attribute { }

[AttributeUsage(AttributeTargets.Property)]
public sealed class IgnoreSelectAttribute : Attribute { }

[AttributeUsage(AttributeTargets.Property)]
public sealed class JsonColumnAttribute : Attribute { }

[AttributeUsage(AttributeTargets.Property)]
public sealed class ProjectionSourceAttribute : Attribute
{
  public Type? ModelType { get; }
  public string? Alias { get; }

  public ProjectionSourceAttribute(Type modelType)
  {
    ModelType = modelType;
  }

  public ProjectionSourceAttribute(string alias)
  {
    Alias = alias;
  }
}

[AttributeUsage(AttributeTargets.Property)]
public sealed class DbColumnAttribute : Attribute
{
  public string ColumnName { get; }
  public bool Selectable { get; set; } = true;
  public bool Writable { get; set; } = true;

  public DbColumnAttribute(string columnName)
  {
    ColumnName = columnName;
  }
}

[AttributeUsage(AttributeTargets.Property)]
public sealed class DbRelationAttribute : Attribute
{
  public Type RelatedType { get; }
  public string LocalKey { get; }
  public string ForeignKey { get; }
  public string? Alias { get; init; }

  public DbRelationAttribute(Type relatedType, string localKey, string foreignKey)
  {
    RelatedType = relatedType;
    LocalKey = localKey;
    ForeignKey = foreignKey;
  }
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class DbTableAttribute(string tableName) : Attribute
{
  public string TableName { get; } = tableName;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public sealed class DbIndexAttribute(params string[] columns) : Attribute
{
  public string[] Columns { get; } = columns;
  public bool Unique { get; init; }
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public sealed class DbConstraintAttribute(string sql) : Attribute
{
  public string Sql { get; } = sql;
}

#endregion
