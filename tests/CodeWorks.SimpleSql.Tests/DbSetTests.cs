using System.Data;
using CodeWorks.SimpleSql;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class DbSetTests
{
  [Fact]
  public void ToCompiledQuery_BuildsWhereOrderAndPagingSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active")
      .OrderBy(x => x.CreatedAt, desc: true)
      .Page(2, 25);

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
    Assert.Contains("ORDER BY t0.\"created_at\" DESC", compiled.Sql);
    Assert.Contains("LIMIT 25 OFFSET 25", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_WithInclude_BuildsRelationshipJoinSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<RepoCustomer>(x => x.Customer)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("LEFT JOIN \"repo_customers\" c1 ON t0.\"customer_id\" = c1.\"id\"", compiled.Sql);
    Assert.Contains("c1.\"name\" AS \"Name\"", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void DbSet_IsImmutable_WhenAddingWhereClause()
  {
    var original = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var filtered = original.Where(x => x.Status == "active");

    var originalSql = original.ToCompiledQuery().Sql;
    var filteredSql = filtered.ToCompiledQuery().Sql;

    Assert.DoesNotContain("WHERE", originalSql);
    Assert.Contains("WHERE", filteredSql);
  }

  [Fact]
  public void ToCompiledQuery_FirstMode_AddsSingleRowLimiter()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active")
      .OrderBy(x => x.CreatedAt);

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.First);

    Assert.Contains("ORDER BY t0.\"created_at\" ASC", compiled.Sql);
    Assert.EndsWith("LIMIT 1", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_CountMode_BuildsCountSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.Count);

    Assert.StartsWith("SELECT COUNT(1) FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_ExistsMode_BuildsExistsSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.Exists);

    Assert.StartsWith("SELECT EXISTS (SELECT 1 FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_ForPostgres_BuildsOnConflictUpdate()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var row = new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.Contains("INSERT INTO \"repo_orders\"", compiled.Sql);
    Assert.Contains("ON CONFLICT (\"id\")", compiled.Sql);
    Assert.Contains("DO UPDATE SET", compiled.Sql);
    Assert.Contains("\"customer_id\" = EXCLUDED.\"customer_id\"", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_ForSqlServer_BuildsMergeStatement()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.SqlServer);
    var row = new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.StartsWith("MERGE [repo_orders] AS target", compiled.Sql);
    Assert.Contains("ON target.[id] = source.[id]", compiled.Sql);
    Assert.Contains("WHEN NOT MATCHED THEN INSERT", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_IncludesParametersForAllInsertColumns()
  {
    var id = Guid.NewGuid();
    var customerId = Guid.NewGuid();
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var row = new RepoOrder
    {
      Id = id,
      CustomerId = customerId,
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.Equal(id, compiled.Parameters.Get<Guid>("Id"));
    Assert.Equal(customerId, compiled.Parameters.Get<Guid>("CustomerId"));
    Assert.Equal("active", compiled.Parameters.Get<string>("Status"));
  }

  [Fact]
  public void ToUpsertCompiledQuery_UsesStableSqlForDifferentRows()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);

    var sqlA = dbSet.ToUpsertCompiledQuery(new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "a"
    }, x => x.Id).Sql;

    var sqlB = dbSet.ToUpsertCompiledQuery(new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "b"
    }, x => x.Id).Sql;

    Assert.Equal(sqlA, sqlB);
  }
}

[DbTable("repo_orders")]
public sealed class RepoOrder
{
  public Guid Id { get; set; }

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  public string Status { get; set; } = string.Empty;
  public DateTime CreatedAt { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(RepoCustomer), nameof(CustomerId), nameof(RepoCustomer.Id), Alias = "c1")]
  public RepoCustomer? Customer { get; set; }
}

[DbTable("repo_customers")]
public sealed class RepoCustomer
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}

internal sealed class FakeConnection : IDbConnection
{
  public string ConnectionString { get; set; } = string.Empty;
  public int ConnectionTimeout => 0;
  public string Database => string.Empty;
  public ConnectionState State => ConnectionState.Closed;

  public IDbTransaction BeginTransaction() => throw new NotSupportedException();
  public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();
  public void ChangeDatabase(string databaseName) => throw new NotSupportedException();
  public void Close() { }
  public IDbCommand CreateCommand() => throw new NotSupportedException();
  public void Open() { }
  public void Dispose() { }
}
