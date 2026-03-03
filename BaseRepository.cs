using System.Data;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;

namespace CodeWorks.SimpleSql;

public interface ISqlConnectionAccessor
{
  ISqlDialect Dialect { get; }
  Task<IDbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
}

public abstract class BaseRepository
{
  private readonly ISqlConnectionAccessor _connectionAccessor;

  protected BaseRepository(ISqlConnectionAccessor connectionAccessor)
  {
    _connectionAccessor = connectionAccessor;
  }

  protected async Task<TResult> WithSessionAsync<TResult>(
    Func<SqlSession, Task<TResult>> action,
    CancellationToken cancellationToken = default)
  {
    var db = await _connectionAccessor.OpenConnectionAsync(cancellationToken);
    try
    {
      var session = new SqlSession(db, _connectionAccessor.Dialect);
      return await action(session);
    }
    finally
    {
      await DisposeAsync(db);
    }
  }

  protected async Task WithSessionAsync(
    Func<SqlSession, Task> action,
    CancellationToken cancellationToken = default)
  {
    var db = await _connectionAccessor.OpenConnectionAsync(cancellationToken);
    try
    {
      var session = new SqlSession(db, _connectionAccessor.Dialect);
      await action(session);
    }
    finally
    {
      await DisposeAsync(db);
    }
  }

  protected async Task<TResult> WithConnectionAsync<TResult>(
    Func<IDbConnection, Task<TResult>> action,
    CancellationToken cancellationToken = default)
  {
    var db = await _connectionAccessor.OpenConnectionAsync(cancellationToken);
    try
    {
      return await action(db);
    }
    finally
    {
      await DisposeAsync(db);
    }
  }

  protected async Task WithConnectionAsync(
    Func<IDbConnection, Task> action,
    CancellationToken cancellationToken = default)
  {
    var db = await _connectionAccessor.OpenConnectionAsync(cancellationToken);
    try
    {
      await action(db);
    }
    finally
    {
      await DisposeAsync(db);
    }
  }

  protected async Task WithTransactionAsync(
    Func<SqlSession, IDbTransaction, Task> action,
    CancellationToken cancellationToken = default)
  {
    var db = await _connectionAccessor.OpenConnectionAsync(cancellationToken);
    IDbTransaction? tx = null;

    try
    {
      tx = await BeginTransactionAsync(db, cancellationToken);

      var session = new SqlSession(db, _connectionAccessor.Dialect);
      await action(session, tx);

      await CommitTransactionAsync(tx, cancellationToken);
    }
    finally
    {
      if (tx != null)
        await DisposeAsync(tx);

      await DisposeAsync(db);
    }
  }

  private static async Task<IDbTransaction> BeginTransactionAsync(IDbConnection db, CancellationToken cancellationToken)
  {
    if (db is DbConnection dbConnection)
      return await dbConnection.BeginTransactionAsync(cancellationToken);

    return db.BeginTransaction();
  }

  private static async Task CommitTransactionAsync(IDbTransaction tx, CancellationToken cancellationToken)
  {
    if (tx is DbTransaction dbTransaction)
    {
      await dbTransaction.CommitAsync(cancellationToken);
      return;
    }

    tx.Commit();
  }

  private static async ValueTask DisposeAsync(object value)
  {
    if (value is IAsyncDisposable asyncDisposable)
    {
      await asyncDisposable.DisposeAsync();
      return;
    }

    if (value is IDisposable disposable)
      disposable.Dispose();
  }
}

public static class BaseRepositoryServiceCollectionExtensions
{
  public static IServiceCollection AddBaseRepositoriesFromAssemblyContaining<TMarker>(
    this IServiceCollection services,
    ServiceLifetime lifetime = ServiceLifetime.Scoped)
    => services.AddBaseRepositoriesFromAssembly(typeof(TMarker).Assembly, lifetime);

  public static IServiceCollection AddBaseRepositoriesFromAssembly(
    this IServiceCollection services,
    System.Reflection.Assembly assembly,
    ServiceLifetime lifetime = ServiceLifetime.Scoped)
  {
    ArgumentNullException.ThrowIfNull(services);
    ArgumentNullException.ThrowIfNull(assembly);

    var repositoryTypes = assembly
      .DefinedTypes
      .Where(type =>
        type is { IsClass: true, IsAbstract: false } &&
        typeof(BaseRepository).IsAssignableFrom(type.AsType()))
      .Select(type => type.AsType())
      .ToList();

    foreach (var repositoryType in repositoryTypes)
    {
      var contracts = repositoryType
        .GetInterfaces()
        .Where(type => type.Name.EndsWith("Repository", StringComparison.Ordinal))
        .ToList();

      if (contracts.Count == 0)
      {
        services.Add(new ServiceDescriptor(repositoryType, repositoryType, lifetime));
        continue;
      }

      foreach (var contract in contracts)
      {
        services.Add(new ServiceDescriptor(contract, repositoryType, lifetime));
      }
    }

    return services;
  }
}
