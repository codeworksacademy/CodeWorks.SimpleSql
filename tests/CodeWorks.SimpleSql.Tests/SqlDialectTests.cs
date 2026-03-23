using System.Data;
using System.Reflection;
using CodeWorks.SimpleSql;
using Xunit;

namespace CodeWorks.SimpleSql.Tests
{
  public class SqlDialectTests
  {
    [Fact]
    public void Detect_ReturnsPostgres_ForNpgsqlLikeConnectionType()
    {
      var dialect = SqlDialects.Detect(new Npgsql.FakeNpgsqlConnection());
      Assert.Equal("postgres", dialect.Name);
    }

    [Fact]
    public void Detect_ReturnsSqlServer_ForSqlClientLikeConnectionType()
    {
      var dialect = SqlDialects.Detect(new Microsoft.Data.SqlClient.FakeSqlClientConnection());
      Assert.Equal("sqlserver", dialect.Name);
    }

    [Fact]
    public void Detect_DefaultsToPostgres_ForUnknownConnectionType()
    {
      var dialect = SqlDialects.Detect(new UnknownConnection());
      Assert.Equal("postgres", dialect.Name);
    }

    [Fact]
    public void BuildSqlType_Postgres_UsesVectorTypeWithDimensions()
    {
      var prop = typeof(VectorEntity).GetProperty(nameof(VectorEntity.Embedding), BindingFlags.Public | BindingFlags.Instance)!;

      var sqlType = SqlDialects.Postgres.BuildSqlType(prop.PropertyType, prop);

      Assert.Equal("VECTOR(1536)", sqlType);
    }

    [Fact]
    public void BuildSqlType_SqlServer_FallsBackToNVarCharForVector()
    {
      var prop = typeof(VectorEntity).GetProperty(nameof(VectorEntity.Embedding), BindingFlags.Public | BindingFlags.Instance)!;

      var sqlType = SqlDialects.SqlServer.BuildSqlType(prop.PropertyType, prop);

      Assert.Equal("NVARCHAR(MAX)", sqlType);
    }

    [Fact]
    public void BuildSqlType_UsesExplicitVectorSqlType_WhenConfigured()
    {
      var prop = typeof(CustomVectorEntity).GetProperty(nameof(CustomVectorEntity.Embedding), BindingFlags.Public | BindingFlags.Instance)!;

      var postgresSqlType = SqlDialects.Postgres.BuildSqlType(prop.PropertyType, prop);
      var sqlServerSqlType = SqlDialects.SqlServer.BuildSqlType(prop.PropertyType, prop);

      Assert.Equal("HALFVEC(768)", postgresSqlType);
      Assert.Equal("HALFVEC(768)", sqlServerSqlType);
    }

    [Fact]
    public void BuildSqlType_Postgres_UsesTsVector_ForDbSearchVector()
    {
      var prop = typeof(SearchVectorEntity).GetProperty(nameof(SearchVectorEntity.SearchVector), BindingFlags.Public | BindingFlags.Instance)!;

      var sqlType = SqlDialects.Postgres.BuildSqlType(prop.PropertyType, prop);

      Assert.Equal("TSVECTOR", sqlType);
    }
  }

  public sealed class VectorEntity
  {
    [DbVector(1536)]
    public float[] Embedding { get; set; } = [];
  }

  public sealed class CustomVectorEntity
  {
    [DbVector(SqlType = "HALFVEC(768)")]
    public float[] Embedding { get; set; } = [];
  }

  public sealed class SearchVectorEntity
  {
    [DbSearchVector(nameof(Name))]
    public string SearchVector { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;
  }

  internal sealed class UnknownConnection : IDbConnection
  {
    public string? ConnectionString { get; set; }
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
}

namespace Npgsql
{
  internal sealed class FakeNpgsqlConnection : IDbConnection
  {
    public string? ConnectionString { get; set; }
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
}

namespace Microsoft.Data.SqlClient
{
  internal sealed class FakeSqlClientConnection : IDbConnection
  {
    public string? ConnectionString { get; set; }
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
}
