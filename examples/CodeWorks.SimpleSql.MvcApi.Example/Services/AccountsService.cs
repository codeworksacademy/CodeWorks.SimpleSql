using CodeWorks.SimpleSql.MvcApi.Example.Repositories;

namespace CodeWorks.SimpleSql.MvcApi.Example.Services;

public interface IAccountsService
{
  Task<List<Account>> GetAccountsAsync();
  Task<List<Account>> GetRichAccountsAsync();
  Task<List<PublicProfile>> GetPublicProfilesAsync();
  Task<List<AccountSummaryProjection>> GetAccountSummariesAsync();
  Task<Guid> UpsertAccountAsync(UpsertAccountInput input);
}

public sealed class AccountsService(IAccountsRepository repository) : IAccountsService
{
  private readonly IAccountsRepository _repository = repository;

  public Task<List<Account>> GetAccountsAsync() => _repository.GetAccountsAsync();

  public Task<List<Account>> GetRichAccountsAsync() => _repository.GetRichAccountsAsync();

  public Task<List<PublicProfile>> GetPublicProfilesAsync() => _repository.GetPublicProfilesAsync();

  public Task<List<AccountSummaryProjection>> GetAccountSummariesAsync() => _repository.GetAccountSummariesAsync();

  public async Task<Guid> UpsertAccountAsync(UpsertAccountInput input)
  {
    var email = (input.Email ?? string.Empty).Trim();
    var displayName = (input.DisplayName ?? string.Empty).Trim();

    if (string.IsNullOrWhiteSpace(email))
      throw new ArgumentException("Email is required.", nameof(input));

    if (string.IsNullOrWhiteSpace(displayName))
      throw new ArgumentException("DisplayName is required.", nameof(input));

    var model = new Account
    {
      Id = input.Id == Guid.Empty ? Guid.NewGuid() : input.Id,
      Email = email.ToLowerInvariant(),
      DisplayName = displayName,
      Active = input.Active,
      OwnerId = input.OwnerId,
      ManagerId = input.ManagerId
    };

    await _repository.UpsertAccountAsync(model);
    return model.Id;
  }
}

public sealed class UpsertAccountInput
{
  public Guid Id { get; set; }
  public string Email { get; set; } = string.Empty;
  public string DisplayName { get; set; } = string.Empty;
  public bool Active { get; set; }
  public Guid OwnerId { get; set; }
  public Guid ManagerId { get; set; }
}
