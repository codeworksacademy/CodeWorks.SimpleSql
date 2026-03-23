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

    Assert.Contains("LIKE LOWER(@q_like_0)", whereSql);
    Assert.DoesNotContain("||", whereSql);
    Assert.Null(rankSql);
    Assert.Equal("%jake%", parameters.Get<string>("q_like_0"));
    Assert.Equal("jake%", parameters.Get<string>("q_prefix_0"));
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

    Assert.Contains("\"search_vector\" @@", whereSql);
    Assert.NotNull(rankSql);
    Assert.Contains("ts_rank(\"search_vector\"", rankSql);
  }

  [Fact]
  public void BuildFilter_AutoDetectsDbSearchVector_AndSupportsCombinedNameTokens()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<PersonSearchEntity>(
      keyword: "john doe",
      filters: null,
      parameters: parameters,
      dialect: SqlDialects.Postgres
    );

    Assert.Contains("\"person_search_vector\" @@", whereSql);
    Assert.NotNull(rankSql);
    Assert.Contains("ts_rank(\"person_search_vector\"", rankSql);
    Assert.Equal("john doe", parameters.Get<string>("q"));
  }

  [Fact]
  public void BuildFilter_AutoDetectsDbSearchVector_PhraseMode_UsesPhraseTsQuery()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<PersonSearchEntity>(
      keyword: "john doe",
      filters: null,
      parameters: parameters,
      dialect: SqlDialects.Postgres,
      searchOptions: new SqlSearchOptions { Mode = SqlSearchMode.Phrase }
    );

    Assert.Contains("\"person_search_vector\" @@", whereSql);
    Assert.Contains("phraseto_tsquery('simple', @q)", whereSql);
    Assert.NotNull(rankSql);
    Assert.Contains("phraseto_tsquery('simple', @q)", rankSql);
  }

  [Fact]
  public void BuildFilter_AutoDetectsDbSearchVector_PlainMode_UsesPlainTsQuery()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<PersonSearchEntity>(
      keyword: "john doe",
      filters: null,
      parameters: parameters,
      dialect: SqlDialects.Postgres,
      searchOptions: new SqlSearchOptions { Mode = SqlSearchMode.Plain }
    );

    Assert.Contains("\"person_search_vector\" @@", whereSql);
    Assert.Contains("plainto_tsquery('simple', @q)", whereSql);
    Assert.NotNull(rankSql);
    Assert.Contains("plainto_tsquery('simple', @q)", rankSql);
  }

  [Fact]
  public void BuildFilter_AutoDetectsDbSearchVector_UsesSourceFieldsForNonPostgresFallback()
  {
    var parameters = new DynamicParameters();

    var (whereSql, rankSql) = SqlHelper.BuildFilter<PersonSearchEntity>(
      keyword: "john doe",
      filters: null,
      parameters: parameters,
      dialect: SqlDialects.SqlServer
    );

    Assert.Contains("[first_name]", whereSql);
    Assert.Contains("[last_name]", whereSql);
    Assert.DoesNotContain("person_search_vector @@", whereSql);
    Assert.Null(rankSql);
  }

  [Fact]
  public void SqlPropertyMap_DbSearchVector_IsNotWritableOrSelectableByDefault()
  {
    var map = SQLMapper.Get<PersonSearchEntity>();
    var searchVector = map.Properties.Single(p => p.Property.Name == nameof(PersonSearchEntity.SearchVector));

    Assert.True(searchVector.IsSearchVector);
    Assert.False(searchVector.Writable);
    Assert.False(searchVector.Selectable);
  }

  [Fact]
  public void ResolveTableName_UsesAttributeValue()
  {
    var tableName = SqlHelper.ResolveTableName(typeof(TestEntity));
    Assert.Equal("test_entities", tableName);
  }

  [Fact]
  public void ToParameters_FormatsVectorValues()
  {
    var parameters = SqlHelper.ToParameters(new TestVectorEntity
    {
      Id = Guid.NewGuid(),
      Embedding = [1f, 2f, 3f]
    });

    Assert.Equal("[1,2,3]", parameters.Get<string>("Embedding"));
  }
}

[DbTable("test_entities")]
public sealed class TestEntity
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}

[DbTable("test_vectors")]
public sealed class TestVectorEntity
{
  public Guid Id { get; set; }

  [DbVector(3)]
  public float[] Embedding { get; set; } = [];
}

[DbTable("people")]
public sealed class PersonSearchEntity
{
  public Guid Id { get; set; }

  [DbColumn("first_name")]
  public string FirstName { get; set; } = string.Empty;

  [DbColumn("last_name")]
  public string LastName { get; set; } = string.Empty;

  [IgnoreWrite]
  [IgnoreSelect]
  public string Name => $"{FirstName} {LastName}";

  [DbColumn("person_search_vector")]
  [DbSearchVector(nameof(FirstName), nameof(LastName))]
  public string SearchVector { get; set; } = string.Empty;
}
