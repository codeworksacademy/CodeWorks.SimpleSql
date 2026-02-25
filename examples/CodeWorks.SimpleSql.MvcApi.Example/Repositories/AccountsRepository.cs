using CodeWorks.SimpleSql;
using Npgsql;

namespace CodeWorks.SimpleSql.MvcApi.Example.Repositories;

public interface IAccountsRepository
{
    Task<List<PublicProfile>> GetPublicProfilesAsync();
    Task<List<AccountSummaryProjection>> GetAccountSummariesAsync();
    Task UpsertAccountAsync(Account account);
}

public sealed class AccountsRepository(NpgsqlDataSource dataSource) : IAccountsRepository
{
    private readonly NpgsqlDataSource _dataSource = dataSource;

    public Task<List<PublicProfile>> GetPublicProfilesAsync() =>
      WithSession(async session =>
        await session
          .Set<Account>()
          .Where(x => x.Active)
          .Select<PublicProfile>()
          .ToListAsync());

    public Task<List<AccountSummaryProjection>> GetAccountSummariesAsync() =>
      WithSession(async session =>
        await session
          .Set<Account>()
          .Include<User>(x => x.Owner, alias: "owner")
          .Include<User>(x => x.Manager, alias: "manager")
          .Select<AccountSummaryProjection>()
          .ToListAsync());

    public Task UpsertAccountAsync(Account account) =>
      WithConnection(async db =>
      {
          await using var tx = await db.BeginTransactionAsync();
          var session = new SqlSession(db, SqlDialects.Postgres);

          await session
          .Set<Account>()
          .UpsertAsync(account, x => x.Id, tx);

          await tx.CommitAsync();
      });

    private async Task<List<T>> WithSession<T>(Func<SqlSession, Task<List<T>>> action)
    {
        await using var db = await _dataSource.OpenConnectionAsync();
        var session = new SqlSession(db, SqlDialects.Postgres);
        return await action(session);
    }

    private async Task WithConnection(Func<NpgsqlConnection, Task> action)
    {
        await using var db = await _dataSource.OpenConnectionAsync();
        await action(db);
    }
}

[DbTable("accounts")]
public sealed class Account
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;

    [DbColumn("display_name")]
    public string DisplayName { get; set; } = string.Empty;

    public bool Active { get; set; }

    [DbColumn("owner_id")]
    public Guid OwnerId { get; set; }

    [DbColumn("manager_id")]
    public Guid ManagerId { get; set; }

    [IgnoreWrite]
    [DbRelation(typeof(User), nameof(OwnerId), nameof(User.Id), Alias = "owner")]
    public User? Owner { get; set; }

    [IgnoreWrite]
    [DbRelation(typeof(User), nameof(ManagerId), nameof(User.Id), Alias = "manager")]
    public User? Manager { get; set; }
}

[DbTable("accounts")]
public sealed class PublicProfile
{
    public Guid Id { get; set; }

    [DbColumn("display_name")]
    public string DisplayName { get; set; } = string.Empty;
}

[DbTable("users")]
public sealed class User
{
    public Guid Id { get; set; }

    [DbColumn("name")]
    public string Name { get; set; } = string.Empty;
}

public sealed class AccountSummaryProjection
{
    [DbColumn("display_name")]
    public string AccountName { get; set; } = string.Empty;

    [DbColumn("name")]
    [ProjectionSource("owner")]
    public string OwnerName { get; set; } = string.Empty;

    [DbColumn("name")]
    [ProjectionSource("manager")]
    public string ManagerName { get; set; } = string.Empty;
}
