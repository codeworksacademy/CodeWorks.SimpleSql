using System.Reflection;
using CodeWorks.SimpleSql;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class SchemaSyncTests
{
  [Fact]
  public void BuildAddColumnSql_ForPostgresSearchVector_UsesGeneratedStoredExpression()
  {
    var prop = typeof(SearchDocument).GetProperty(nameof(SearchDocument.SearchVector), BindingFlags.Public | BindingFlags.Instance)!;

    var sql = SearchVectorSchemaSqlBuilder.BuildAddColumnSql(
      typeof(SearchDocument),
      prop,
      "public",
      "search_documents",
      SqlDialects.Postgres);

    Assert.Contains("ADD COLUMN \"search_vector\" TSVECTOR GENERATED ALWAYS AS", sql);
    Assert.Contains("to_tsvector('simple'", sql);
    Assert.Contains("COALESCE(CAST(\"first_name\" AS TEXT), '')", sql);
    Assert.Contains("COALESCE(CAST(\"last_name\" AS TEXT), '')", sql);
    Assert.Contains("STORED", sql);
  }

  [Fact]
  public void BuildIndexSql_ForPostgresSearchVector_UsesGinIndex()
  {
    var prop = typeof(SearchDocument).GetProperty(nameof(SearchDocument.SearchVector), BindingFlags.Public | BindingFlags.Instance)!;

    var sql = SearchVectorSchemaSqlBuilder.BuildIndexSql(
      typeof(SearchDocument),
      prop,
      "public",
      "search_documents",
      SqlDialects.Postgres,
      "idx_search_documents_search_vector");

    Assert.NotNull(sql);
    Assert.Contains("CREATE INDEX IF NOT EXISTS \"idx_search_documents_search_vector\"", sql);
    Assert.Contains("USING GIN (\"search_vector\")", sql);
  }

  [Fact]
  public void BuildIndexSql_ForSqlServerSearchVector_ReturnsNull()
  {
    var prop = typeof(SearchDocument).GetProperty(nameof(SearchDocument.SearchVector), BindingFlags.Public | BindingFlags.Instance)!;

    var sql = SearchVectorSchemaSqlBuilder.BuildIndexSql(
      typeof(SearchDocument),
      prop,
      "dbo",
      "search_documents",
      SqlDialects.SqlServer,
      "idx_search_documents_search_vector");

    Assert.Null(sql);
  }

  [Fact]
  public void BuildAddColumnSql_ForPostgresWeightedSearchVector_UsesSetWeightPerSource()
  {
    var prop = typeof(WeightedSearchDocument).GetProperty(nameof(WeightedSearchDocument.SearchVector), BindingFlags.Public | BindingFlags.Instance)!;

    var sql = SearchVectorSchemaSqlBuilder.BuildAddColumnSql(
      typeof(WeightedSearchDocument),
      prop,
      "public",
      "weighted_search_documents",
      SqlDialects.Postgres);

    Assert.Contains("setweight(to_tsvector('simple', COALESCE(CAST(\"title\" AS TEXT), '')), 'A')", sql);
    Assert.Contains("setweight(to_tsvector('simple', COALESCE(CAST(\"body\" AS TEXT), '')), 'B')", sql);
  }
}

[DbTable("search_documents")]
public sealed class SearchDocument
{
  public Guid Id { get; set; }

  [DbColumn("first_name")]
  public string FirstName { get; set; } = string.Empty;

  [DbColumn("last_name")]
  public string LastName { get; set; } = string.Empty;

  [DbColumn("search_vector")]
  [DbSearchVector(nameof(FirstName), nameof(LastName))]
  public string SearchVector { get; set; } = string.Empty;
}

[DbTable("weighted_search_documents")]
public sealed class WeightedSearchDocument
{
  public Guid Id { get; set; }

  [DbColumn("title")]
  public string Title { get; set; } = string.Empty;

  [DbColumn("body")]
  public string Body { get; set; } = string.Empty;

  [DbColumn("search_vector")]
  [DbSearchVector(nameof(Title), nameof(Body), SourceWeights = ["A", "B"])]
  public string SearchVector { get; set; } = string.Empty;
}