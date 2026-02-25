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
