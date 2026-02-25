using CodeWorks.SimpleSql.MvcApi.Example.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace CodeWorks.SimpleSql.MvcApi.Example.Controllers;

[ApiController]
[Route("api/accounts")]
public sealed class AccountsController(IAccountsRepository repository) : ControllerBase
{
    private readonly IAccountsRepository _repository = repository;

    [HttpGet]
    public async Task<ActionResult<List<Account>>> GetAll()
    {
        List<Account> rows = await _repository.GetRichAccountsAsync();
        return Ok(rows);
    }

    [HttpGet("profiles")]
    public async Task<ActionResult<List<PublicProfile>>> GetProfiles()
    {
        var rows = await _repository.GetPublicProfilesAsync();
        return Ok(rows);
    }

    [HttpGet("summaries")]
    public async Task<ActionResult<List<AccountSummaryProjection>>> GetSummaries()
    {
        var rows = await _repository.GetAccountSummariesAsync();
        return Ok(rows);
    }

    [HttpPost("upsert")]
    public async Task<IActionResult> Upsert([FromBody] UpsertAccountRequest request)
    {
        var row = new Account
        {
            Id = request.Id == Guid.Empty ? Guid.NewGuid() : request.Id,
            Email = request.Email,
            DisplayName = request.DisplayName,
            Active = request.Active,
            OwnerId = request.OwnerId,
            ManagerId = request.ManagerId
        };

        await _repository.UpsertAccountAsync(row);
        return Ok(new { row.Id });
    }
}

public sealed class UpsertAccountRequest
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public bool Active { get; set; }
    public Guid OwnerId { get; set; }
    public Guid ManagerId { get; set; }
}
