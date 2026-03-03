using System.Data;
using CodeWorks.SimpleSql;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class BaseRepositoryRegistrationTests
{
  [Fact]
  public void AddBaseRepositoriesFromAssemblyContaining_RegistersRepositoryByInterface()
  {
    var services = new ServiceCollection();

    services.AddBaseRepositoriesFromAssemblyContaining<TestRepository>();

    var descriptor = Assert.Single(services, s => s.ServiceType == typeof(ITestRepository));
    Assert.Equal(typeof(TestRepository), descriptor.ImplementationType);
    Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
  }

  [Fact]
  public void AddBaseRepositoriesFromAssemblyContaining_UsesRequestedLifetime()
  {
    var services = new ServiceCollection();

    services.AddBaseRepositoriesFromAssemblyContaining<TestRepository>(ServiceLifetime.Singleton);

    var descriptor = Assert.Single(services, s => s.ServiceType == typeof(ITestRepository));
    Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
  }

  private sealed class FakeConnectionAccessor : ISqlConnectionAccessor
  {
    public ISqlDialect Dialect => SqlDialects.Postgres;

    public Task<IDbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
      => throw new NotImplementedException();
  }

  private interface ITestRepository;

  private sealed class TestRepository(ISqlConnectionAccessor connectionAccessor)
    : BaseRepository(connectionAccessor), ITestRepository;
}
