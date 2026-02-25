using CodeWorks.SimpleSql;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class SqlQueryBuilderTests
{
  [Fact]
  public void BuildSelectFrom_WithoutInclude_UsesRootTableAndAlias()
  {
    var query = SqlHelper.Query<QueryOrder>(dialect: SqlDialects.Postgres);

    var sql = query.BuildSelectFrom();

    Assert.Contains("FROM \"query_orders\" t0", sql);
    Assert.DoesNotContain("JOIN", sql);
    Assert.Contains("t0.\"id\" AS \"Id\"", sql);
    Assert.Contains("t0.\"customer_id\" AS \"CustomerId\"", sql);
  }

  [Fact]
  public void Include_BuildsLeftJoinAndAddsRelatedSelectColumns()
  {
    var query = SqlHelper.Query<QueryOrder>(dialect: SqlDialects.Postgres)
      .Include<QueryCustomer>(x => x.Customer);

    var fromSql = query.BuildFrom();
    var selectSql = query.BuildSelect();

    Assert.Contains("LEFT JOIN \"query_customers\" c1 ON t0.\"customer_id\" = c1.\"id\"", fromSql);
    Assert.Contains("c1.\"name\" AS \"Name\"", selectSql);
  }

  [Fact]
  public void Include_WithInnerJoinAndCustomAlias_UsesProvidedJoinAndAlias()
  {
    var query = SqlHelper.Query<QueryOrder>(dialect: SqlDialects.Postgres)
      .Include<QueryCustomer>(x => x.Customer, SqlJoinType.Inner, alias: "cust");

    var fromSql = query.BuildFrom();

    Assert.Contains("INNER JOIN \"query_customers\" cust ON t0.\"customer_id\" = cust.\"id\"", fromSql);
  }

  [Fact]
  public void Include_Throws_WhenNavigationMissingDbRelation()
  {
    var query = SqlHelper.Query<QueryOrderWithoutRelation>(dialect: SqlDialects.Postgres);

    var ex = Assert.Throws<InvalidOperationException>(() =>
      query.Include<QueryCustomer>(x => x.Customer));

    Assert.Contains("missing [DbRelation]", ex.Message);
  }

  [Fact]
  public void Include_Throws_WhenRelatedTypeDoesNotMatchGenericType()
  {
    var query = SqlHelper.Query<QueryOrderWithMismatchRelation>(dialect: SqlDialects.Postgres);

    var ex = Assert.Throws<InvalidOperationException>(() =>
      query.Include<QueryCustomer>(x => x.Target));

    Assert.Contains("type mismatch", ex.Message);
  }
}

[DbTable("query_orders")]
public sealed class QueryOrder
{
  public Guid Id { get; set; }

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(QueryCustomer), nameof(CustomerId), nameof(QueryCustomer.Id), Alias = "c1")]
  public QueryCustomer? Customer { get; set; }
}

[DbTable("query_customers")]
public sealed class QueryCustomer
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}

[DbTable("query_orders_without_relation")]
public sealed class QueryOrderWithoutRelation
{
  public Guid Id { get; set; }
  public Guid CustomerId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  public QueryCustomer? Customer { get; set; }
}

[DbTable("query_orders_mismatch")]
public sealed class QueryOrderWithMismatchRelation
{
  public Guid Id { get; set; }
  public Guid CustomerId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(QueryAddress), nameof(CustomerId), nameof(QueryAddress.Id))]
  public QueryAddress? Target { get; set; }
}

[DbTable("query_addresses")]
public sealed class QueryAddress
{
  public Guid Id { get; set; }
}
