# CodeWorks.SimpleSql

`CodeWorks.SimpleSql` is a lightweight SQL helper library with:

- SQL generation from strongly-typed models and expressions
- Dialect support for PostgreSQL and SQL Server
- Convention + attribute-based mapping
- Schema synchronization helpers for tables, columns, indexes, and constraints
- Repository-style `DbSet<T>` query/write abstraction over Dapper

## Install

```bash
dotnet add package CodeWorks.SimpleSql
```

## Quick start

```csharp
SqlHelper.UseDialect(SqlDialects.Postgres);

var sql = SqlHelper.For<MyEntity>();
var insertColumns = sql.InsertColumns;
var insertValues = sql.InsertValues;
```

## Query + repository-style usage

```csharp
using var db = /* your IDbConnection */;
var session = new SqlSession(db);

var orders = await session
  .Set<Order>()
  .Where(x => x.Status == "active")
  .OrderBy(x => x.CreatedAt, desc: true)
  .Page(1, 50)
  .ToListAsync();

var first = await session
  .Set<Order>()
  .Where(x => x.Status == "active")
  .FirstOrDefaultAsync();

var count = await session
  .Set<Order>()
  .Where(x => x.Status == "active")
  .CountAsync();

var any = await session
  .Set<Order>()
  .Where(x => x.Status == "active")
  .AnyAsync();
```

## Base repository + wide registration

`BaseRepository` provides shared connection/session/transaction helpers for repositories.

```csharp
public interface IOrdersRepository
{
  Task<List<Order>> GetOpenAsync();
}

public sealed class OrdersRepository(ISqlConnectionAccessor accessor)
  : BaseRepository(accessor), IOrdersRepository
{
  public Task<List<Order>> GetOpenAsync() =>
    WithSessionAsync(session =>
      session
        .Set<Order>()
        .Where(x => x.Status == "open")
        .ToListAsync());
}
```

Register all repositories (that inherit `BaseRepository`) from an assembly in one call:

```csharp
services.AddScoped<ISqlConnectionAccessor, MyConnectionAccessor>();
services.AddBaseRepositoriesFromAssemblyContaining<OrdersRepository>();
```

This supports broad repository registration while keeping one-off query logic in derived repositories and business rules in a service layer.

### Full model vs projection model (same table)

```csharp
[DbTable("accounts")]
public class Account
{
  public Guid Id { get; set; }
  public string Email { get; set; } = string.Empty;

  [DbColumn("display_name")]
  public string DisplayName { get; set; } = string.Empty;
}

[DbTable("accounts")]
public class PublicProfile
{
  public Guid Id { get; set; }

  [DbColumn("display_name")]
  public string DisplayName { get; set; } = string.Empty;
}

var profiles = await session
  .Set<Account>()
  .Where(x => x.Active)
  .Select<PublicProfile>()
  .ToListAsync();
```

`Select<TProjection>()` builds SQL from the projection model and only selects mapped projection fields.
Projection selection respects `[IgnoreSelect]` on the projection type.

### Relationship include

```csharp
var withCustomer = await session
  .Set<Order>()
  .Include<Customer>(x => x.Customer)
  .Where(x => x.Status == "active")
  .ToListAsync();
```

Projected queries with `Include(...)` are supported for root + included selectable columns.
When a projection column exists on multiple sources, add `[ProjectionSource(...)]` on the projection property.

```csharp
public class AccountSummaryProjection
{
  [DbColumn("name")]
  [ProjectionSource("owner")]
  public string OwnerName { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource("manager")]
  public string ManagerName { get; set; } = string.Empty;
}

var result = await session
  .Set<Account>()
  .Include<User>(x => x.Owner, alias: "owner")
  .Include<User>(x => x.Manager, alias: "manager")
  .Select<AccountSummaryProjection>()
  .ToListAsync();
```

`ProjectionSource` can target an include alias (`"owner"`) or a model type (`typeof(User)`) when only one source of that type exists.

### Upsert / UpsertMany

```csharp
await session
  .Set<MonthlyRevenue>()
  .UpsertAsync(
    row,
    x => new { x.BusinessId, x.Year, x.Month });

await session
  .Set<MonthlyRevenue>()
  .UpsertManyAsync(
    rows,
    x => new { x.BusinessId, x.Year, x.Month },
    batchSize: 500);
```

`UpsertAsync` and `UpsertManyAsync` are dialect-aware:

- PostgreSQL: `INSERT ... ON CONFLICT ...`
- SQL Server: `MERGE ...`

## Schema sync

```csharp
await SchemaSync.SyncModelsAsync(
  db,
  tx,
  new[] { typeof(MyEntity), typeof(AnotherEntity) },
  options: new SchemaSyncOptions
  {
    LogPath = "/absolute/path/to/db-sync.log",
    EnableConsoleLogging = true
  }
);
```

`LogPath` is optional; when omitted, no file is written.

## Operational notes

- Connection pooling is provided by your database provider, not by this library.
- For PostgreSQL in production, prefer a shared `NpgsqlDataSource` and open scoped connections from it.
- Keep related write operations inside a single `IDbTransaction` and pass the same transaction object to all calls.
- `DbSet<T>` and `SchemaSync` enforce transaction/connection matching and throw if a transaction from a different connection is supplied.

## Example project (real DB)

Use [examples/CodeWorks.SimpleSql.Example/Program.cs](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/examples/CodeWorks.SimpleSql.Example/Program.cs) to test features against PostgreSQL.

```bash
export SIMPLESQL_EXAMPLE_CONNECTION="Host=localhost;Port=5432;Database=app_db;Username=postgres;Password=postgres"
dotnet run --project examples/CodeWorks.SimpleSql.Example/CodeWorks.SimpleSql.Example.csproj
```

## MVC Web API example

A traditional controller-based API sample is available at:

- [examples/CodeWorks.SimpleSql.MvcApi.Example/Program.cs](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/examples/CodeWorks.SimpleSql.MvcApi.Example/Program.cs)
- [examples/CodeWorks.SimpleSql.MvcApi.Example/Repositories/AccountsRepository.cs](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/examples/CodeWorks.SimpleSql.MvcApi.Example/Repositories/AccountsRepository.cs)
- [examples/CodeWorks.SimpleSql.MvcApi.Example/Services/AccountsService.cs](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/examples/CodeWorks.SimpleSql.MvcApi.Example/Services/AccountsService.cs)
- [examples/CodeWorks.SimpleSql.MvcApi.Example/Controllers/AccountsController.cs](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/examples/CodeWorks.SimpleSql.MvcApi.Example/Controllers/AccountsController.cs)

It demonstrates:

- pooled connection usage via `NpgsqlDataSource`
- schema sync at startup
- repository pattern + controller endpoints
- service-layer orchestration for business rules
- projection models (`Select<TProjection>()`)
- include disambiguation with `[ProjectionSource("alias")]`
- nested include materialization for object graphs (`Include(...).ToListAsync()`)
- upsert writes inside explicit transaction scope

Run it:

```bash
export SIMPLESQL_EXAMPLE_CONNECTION="Host=localhost;Port=5432;Database=app_db;Username=postgres;Password=postgres"
dotnet run --project examples/CodeWorks.SimpleSql.MvcApi.Example/CodeWorks.SimpleSql.MvcApi.Example.csproj
```

Sample endpoints:

- `GET /api/accounts/profiles`
- `GET /api/accounts/summaries`
- `GET /api/accounts/rich`
- `POST /api/accounts/upsert`

## Build, test, pack

```bash
dotnet test
dotnet pack -c Release
```

## Release automation (GitHub Actions)

NuGet publish is automated via [.github/workflows/publish-nuget.yml](https://github.com/codeworksacademy/CodeWorks.SimpleSql/blob/main/.github/workflows/publish-nuget.yml).

- Trigger: push a git tag matching `v*` (example: `v0.1.1`)
- Required repository secret: `NUGET_API_KEY`
- Workflow actions: restore, test, pack, push `.nupkg`, push `.snupkg`

### VS Code release task

Run task: `release: bump and publish next`

- Bumps patch version in `CodeWorks.SimpleSql.csproj` (for example `0.1.0` → `0.1.1`)
- Runs test suite
- Commits the version bump and creates git tag `vX.Y.Z`
- Pushes commit + tag to `origin` (which triggers NuGet publish workflow)
