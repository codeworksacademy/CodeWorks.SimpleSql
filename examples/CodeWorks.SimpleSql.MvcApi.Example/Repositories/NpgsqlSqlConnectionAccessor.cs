using System.Data;
using CodeWorks.SimpleSql;
using Npgsql;

namespace CodeWorks.SimpleSql.MvcApi.Example.Repositories;

public sealed class NpgsqlSqlConnectionAccessor(NpgsqlDataSource dataSource) : ISqlConnectionAccessor
{
  private readonly NpgsqlDataSource _dataSource = dataSource;

  public ISqlDialect Dialect => SqlDialects.Postgres;

  public async Task<IDbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    => await _dataSource.OpenConnectionAsync(cancellationToken);
}
