using System.Data;
using CodeWorks.SimpleSql;
using Dapper;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class SqlHelperTests
{
  [Fact]
  public void ToSnakeCase_ConvertsPascalCase()
  {
    var result = SqlHelper.ToSnakeCase("CreatedAt");
    Assert.Equal("created_at", result);
  }

  [Fact]
  public void ToPascalCase_ConvertsSnakeCase()
  {
    var result = SqlHelper.ToPascalCase("created_at");
    Assert.Equal("CreatedAt", result);
  }

  [Fact]
  public void BuildPaging_ClampsValues()
  {
    var result = SqlHelper.BuildPaging(limit: 999, offset: -5);
    Assert.Equal("LIMIT 500 OFFSET 0", result);
  }

  [Fact]
  public void BuildPaging_ForSqlServer_UsesOffsetFetchSyntax()
  {
    var result = SqlHelper.BuildPaging(
      limit: 25,
      offset: 10,
      dialect: SqlDialects.SqlServer
    );

    Assert.Equal("OFFSET 10 ROWS FETCH NEXT 25 ROWS ONLY", result);
  }

  [Fact]
  public void BuildPaging_ForSqlServer_AddsFallbackOrderByWhenRequested()
  {
    var result = SqlHelper.BuildPaging(
      limit: 25,
      offset: 0,
      dialect: SqlDialects.SqlServer,
      forceOrderByForSqlServer: true
    );

    Assert.Equal("ORDER BY (SELECT 1) OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY", result);
  }

  [Fact]
  public void BuildFilter_ForSqlServer_UsesPortableLikeParameter()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<TestEntity>(
      ["name", "email"],
      keyword: "jake",
      filters: null,
      parameters: parameters,
      hasSearchVector: false,
      dialect: SqlDialects.SqlServer
    );

    Assert.Contains("LIKE LOWER(@q_like)", whereSql);
    Assert.DoesNotContain("||", whereSql);
    Assert.Null(rankSql);
    Assert.Equal("jake", parameters.Get<string>("q"));
    Assert.Equal("%jake%", parameters.Get<string>("q_like"));
  }

  [Fact]
  public void BuildFilter_WithPostgresSearchVector_ProducesRankAndPredicate()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<TestEntity>(
      ["name"],
      keyword: "find this",
      filters: null,
      parameters: parameters,
      hasSearchVector: true,
      searchVectorColumn: "search_vector",
      dialect: SqlDialects.Postgres
    );

    Assert.Contains("search_vector @@", whereSql);
    Assert.NotNull(rankSql);
    Assert.Contains("ts_rank(search_vector", rankSql);
  }

  [Fact]
  public void ResolveTableName_UsesAttributeValue()
  {
    var tableName = SqlHelper.ResolveTableName(typeof(TestEntity));
    Assert.Equal("test_entities", tableName);
  }
}

[DbTable("test_entities")]
public sealed class TestEntity
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}
