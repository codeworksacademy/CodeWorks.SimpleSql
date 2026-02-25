# CodeWorks.SimpleSql

`CodeWorks.SimpleSql` is a lightweight SQL helper library with:

- SQL generation from strongly-typed models and expressions
- Dialect support for PostgreSQL and SQL Server
- Convention + attribute-based mapping
- Schema synchronization helpers for tables, columns, indexes, and constraints

## Install

```bash
dotnet add package CodeWorks.SimpleSql
```

  new[] { typeof(MyEntity), typeof(AnotherEntity) },
  options: new SchemaSyncOptions
  {
    LogPath = "/absolute/path/to/db-sync.log",
    EnableConsoleLogging = true
  }

```csharp
`LogPath` is optional; when omitted, no log file is written.
SqlHelper.UseDialect(SqlDialects.Postgres);

var sql = SqlHelper.For<MyEntity>();
```

## Query + repository-style usage

```csharp
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

### Relationship include

```csharp
var withCustomer = await session
  .Set<Order>()
  .Include<Customer>(x => x.Customer)
  .Where(x => x.Status == "active")
  .ToListAsync();
```

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
  new[] { typeof(MyEntity), typeof(AnotherEntity) }
);
```

## Build, test, pack

```bash
dotnet test

dotnet pack -c Release
```

## Release automation (GitHub Actions)

NuGet publish is automated via [.github/workflows/publish-nuget.yml](.github/workflows/publish-nuget.yml).

- Trigger: push a git tag matching `v*` (example: `v0.1.1`)
- Required repository secret: `NUGET_API_KEY`
- Workflow actions: restore, test, pack, push `.nupkg`, push `.snupkg`

### VS Code release task

Run task: `release: bump and publish next`

- Bumps patch version in `CodeWorks.SimpleSql.csproj` (for example `0.1.0` → `0.1.1`)
- Runs test suite
- Commits the version bump and creates git tag `vX.Y.Z`
- Pushes commit + tag to `origin` (which triggers NuGet publish workflow)
