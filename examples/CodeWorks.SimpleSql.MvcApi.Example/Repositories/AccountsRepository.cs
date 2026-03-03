using CodeWorks.SimpleSql;

namespace CodeWorks.SimpleSql.MvcApi.Example.Repositories;

public interface IAccountsRepository
{
    Task<List<PublicProfile>> GetPublicProfilesAsync();
    Task<List<AccountSummaryProjection>> GetAccountSummariesAsync();
    Task UpsertAccountAsync(Account account);
    Task<List<Account>> GetAccountsAsync();
    Task<List<Account>> GetRichAccountsAsync();
}

public sealed class AccountsRepository(ISqlConnectionAccessor connectionAccessor)
  : BaseRepository(connectionAccessor), IAccountsRepository
{
    public Task<List<PublicProfile>> GetPublicProfilesAsync() =>
      WithSessionAsync(async session =>
        await session
          .Set<Account>()
          .Where(x => x.Active)
          .Select<PublicProfile>()
          .ToListAsync());

    public Task<List<Account>> GetAccountsAsync() =>
      WithSessionAsync(async session =>
        await session
          .Set<Account>()
          .ToListAsync());

    public Task<List<Account>> GetRichAccountsAsync() =>
      WithSessionAsync(async session =>
        await session
          .Set<Account>()
          .Include<User>(x => x.Owner, alias: "owner")
          .Include<User>(x => x.Manager, alias: "manager")
          .ToListAsync());

    public Task<List<AccountSummaryProjection>> GetAccountSummariesAsync() =>
      WithSessionAsync(async session =>
        await session
          .Set<Account>()
          .Include<User>(x => x.Owner, alias: "owner")
          .Include<User>(x => x.Manager, alias: "manager")
          .Select<AccountSummaryProjection>()
          .ToListAsync());

    public Task UpsertAccountAsync(Account account) =>
      WithTransactionAsync(async (session, tx) =>
        await session
          .Set<Account>()
          .UpsertAsync(account, x => x.Id, tx));
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

    [IgnoreWrite, IgnoreSelect]
    [DbRelation(typeof(User), nameof(OwnerId), nameof(User.Id), Alias = "owner")]
    public User? Owner { get; set; }

    [IgnoreWrite, IgnoreSelect]
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

[DbTable("accounts")]
public sealed class User
{
    public Guid Id { get; set; }

    [DbColumn("display_name")]
    public string Name { get; set; } = string.Empty;
}

public sealed class AccountSummaryProjection
{
    [DbColumn("display_name")]
    public string AccountName { get; set; } = string.Empty;

    [DbColumn("display_name")]
    [ProjectionSource("owner")]
    public string OwnerName { get; set; } = string.Empty;

    [DbColumn("display_name")]
    [ProjectionSource("manager")]
    public string ManagerName { get; set; } = string.Empty;
}
