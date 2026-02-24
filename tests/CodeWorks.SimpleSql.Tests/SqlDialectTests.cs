using System.Data;
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
