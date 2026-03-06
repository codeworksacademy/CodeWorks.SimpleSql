using CodeWorks.SimpleSql.MvcApi.Example.Repositories;
using CodeWorks.SimpleSql.MvcApi.Example.Services;
using Microsoft.AspNetCore.Mvc;

namespace CodeWorks.SimpleSql.MvcApi.Example.Controllers;

[ApiController]
[Route("api/accounts")]
public sealed class AccountsController(IAccountsService service) : ControllerBase
{
    private readonly IAccountsService _service = service;

    [HttpGet]
    public async Task<ActionResult<List<Account>>> GetAll()
    {
        List<Account> rows = await _service.GetRichAccountsAsync();
        return Ok(rows);
    }

        [HttpGet("rich")]
        public async Task<ActionResult<List<Account>>> GetRich()
        {
            List<Account> rows = await _service.GetRichAccountsAsync();
            return Ok(rows);
        }

    [HttpGet("profiles")]
    public async Task<ActionResult<List<PublicProfile>>> GetProfiles()
    {
        var rows = await _service.GetPublicProfilesAsync();
        return Ok(rows);
    }

    [HttpGet("summaries")]
    public async Task<ActionResult<List<AccountSummaryProjection>>> GetSummaries()
    {
        var rows = await _service.GetAccountSummariesAsync();
        return Ok(rows);
    }

    [HttpGet("claimable-employees")]
    public async Task<ActionResult<List<ClaimableEmployeeProjection>>> GetClaimableEmployees([FromQuery] string email)
    {
        var rows = await _service.GetClaimableEmployeesByEmailAsync(email);
        return Ok(rows);
    }

    [HttpPost("upsert")]
    public async Task<IActionResult> Upsert([FromBody] UpsertAccountRequest request)
    {
        var id = await _service.UpsertAccountAsync(new UpsertAccountInput
        {
            Id = request.Id,
            Email = request.Email,
            DisplayName = request.DisplayName,
            Active = request.Active,
            OwnerId = request.OwnerId,
            ManagerId = request.ManagerId
        });

        return Ok(new { Id = id });
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
