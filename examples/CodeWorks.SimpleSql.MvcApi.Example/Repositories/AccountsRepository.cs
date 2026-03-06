using CodeWorks.SimpleSql;

namespace CodeWorks.SimpleSql.MvcApi.Example.Repositories;

public interface IAccountsRepository
{
  Task<List<PublicProfile>> GetPublicProfilesAsync();
  Task<List<AccountSummaryProjection>> GetAccountSummariesAsync();
  Task<List<ClaimableEmployeeProjection>> GetClaimableEmployeesByEmailAsync(string email);
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

  public Task<List<ClaimableEmployeeProjection>> GetClaimableEmployeesByEmailAsync(string email) =>
    WithSessionAsync(async session =>
      await session
        .Set<ClaimableEmployee>()
        .Where(e =>
          e.Email == email
          && e.Status == "pending"
          && e.AccountId == null)
        .Include<Business>(e => e.Business, alias: "biz")
        .Select<ClaimableEmployeeProjection>()
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

[DbTable("businesses")]
public class Business
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
  public string Logo { get; set; } = string.Empty;
}

[DbTable("employees")]
public class ClaimableEmployee
{
  public Guid Id { get; set; }
  public string Email { get; set; } = string.Empty;
  public string Status { get; set; } = string.Empty;

  [DbColumn("account_id")]
  public Guid? AccountId { get; set; }

  public string Name { get; set; } = string.Empty;

  [DbColumn("business_id")]
  public Guid BusinessId { get; set; }

  [IgnoreWrite, IgnoreSelect]
  [DbRelation(typeof(Business), nameof(BusinessId), nameof(Business.Id), Alias = "biz")]
  public Business? Business { get; set; }
}

public class ClaimableEmployeeProjection
{
  public Guid Id { get; set; }
  public string Email { get; set; } = string.Empty;
  public string Status { get; set; } = string.Empty;
  public string Name { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource(typeof(Business))]
  public string BusinessName { get; set; } = string.Empty;

  [DbColumn("logo")]
  [ProjectionSource(typeof(Business))]
  public string BusinessLogo { get; set; } = string.Empty;
}



