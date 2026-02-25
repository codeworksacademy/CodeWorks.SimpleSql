using CodeWorks.SimpleSql;
using Dapper;
using Npgsql;

var connectionString = "Host=localhost;Database=TestDb;Username=postgres;Password=localdb;";
connectionString = Environment.GetEnvironmentVariable("SIMPLESQL_EXAMPLE_CONNECTION") ?? connectionString;
if (string.IsNullOrWhiteSpace(connectionString))
{
  Console.WriteLine("Set SIMPLESQL_EXAMPLE_CONNECTION to a PostgreSQL connection string.");
  return;
}

await using var db = new NpgsqlConnection(connectionString);
await db.OpenAsync();

SqlHelper.UseDialect(SqlDialects.Postgres);

await using var tx = await db.BeginTransactionAsync();

await SchemaSync.SyncModelsAsync(
  db,
  tx,
  [
    typeof(ExampleCustomer),
    typeof(ExampleOrder),
    typeof(ExampleMonthlyRevenue),
    typeof(ExampleTeam),
    typeof(ExampleRepresentative)
  ],
  options: new SchemaSyncOptions
  {
    LogPath = Path.Combine(AppContext.BaseDirectory, "logs", "db-sync.log"),
    EnableConsoleLogging = true
  });

var customerId = Guid.NewGuid();
var session = new SqlSession(db, SqlDialects.Postgres);

await session
  .Set<ExampleCustomer>()
  .UpsertAsync(
    new ExampleCustomer { Id = customerId, Name = "Acme Corp" },
    x => x.Id,
    tx);

await session
  .Set<ExampleOrder>()
  .UpsertManyAsync(
    [
      new ExampleOrder { Id = Guid.NewGuid(), CustomerId = customerId, Status = "active", Total = 1200m },
      new ExampleOrder { Id = Guid.NewGuid(), CustomerId = customerId, Status = "active", Total = 450m },
      new ExampleOrder { Id = Guid.NewGuid(), CustomerId = customerId, Status = "archived", Total = 75m }
    ],
    x => x.Id,
    transaction: tx);

await session
  .Set<ExampleMonthlyRevenue>()
  .UpsertAsync(
    new ExampleMonthlyRevenue
    {
      BusinessId = customerId,
      Year = DateTime.UtcNow.Year,
      Month = DateTime.UtcNow.Month,
      Revenue = 1725m
    },
    x => new { x.BusinessId, x.Year, x.Month },
    tx);

var activeOrders = await session
  .Set<ExampleOrder>()
  .Include<ExampleCustomer>(x => x.Customer)
  .Where(x => x.Status == "active")
  .OrderBy(x => x.CreatedAt, desc: true)
  .Page(1, 25)
  .ToListAsync(tx);

var activeCount = await session
  .Set<ExampleOrder>()
  .Where(x => x.Status == "active")
  .CountAsync(tx);

var hasArchived = await session
  .Set<ExampleOrder>()
  .Where(x => x.Status == "archived")
  .AnyAsync(tx);

var publicOrders = await session
  .Set<ExampleOrder>()
  .Include<ExampleCustomer>(x => x.Customer, alias: "cust")
  .Where(x => x.Status == "active")
  .Select<ExampleOrderPublicProjection>()
  .ToListAsync(tx);

var teamViews = await session
  .Set<ExampleTeam>()
  .Include<ExampleRepresentative>(x => x.Representative, alias: "rep")
  .Select<ExampleTeamProjectionByType>()
  .ToListAsync(tx);

await tx.CommitAsync();

Console.WriteLine($"Inserted/updated customer + orders. Active orders: {activeCount}, has archived: {hasArchived}");
Console.WriteLine($"Loaded {activeOrders.Count} active orders (with include SQL path).");
Console.WriteLine($"Loaded {publicOrders.Count} projection rows with alias disambiguation.");
Console.WriteLine($"Loaded {teamViews.Count} projection rows with type disambiguation.");

[DbTable("example_customers")]
public sealed class ExampleCustomer
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}

[DbTable("example_orders")]
[DbIndex("customer_id")]
[DbIndex("status")]
public sealed class ExampleOrder
{
  public Guid Id { get; set; }

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  public string Status { get; set; } = string.Empty;
  public decimal Total { get; set; }
  public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(ExampleCustomer), nameof(CustomerId), nameof(ExampleCustomer.Id), Alias = "c1")]
  public ExampleCustomer? Customer { get; set; }
}

public sealed class ExampleOrderPublicProjection
{
  [DbColumn("status")]
  public string Status { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource("cust")]
  public string CustomerName { get; set; } = string.Empty;
}

[DbTable("example_monthly_revenue")]
[DbConstraint("CONSTRAINT uq_example_monthly_revenue UNIQUE (business_id, year, month)")]
public sealed class ExampleMonthlyRevenue
{
  [DbColumn("business_id")]
  public Guid BusinessId { get; set; }

  public int Year { get; set; }
  public int Month { get; set; }
  public decimal Revenue { get; set; }
}

[DbTable("example_teams")]
public sealed class ExampleTeam
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;

  [DbColumn("representative_id")]
  public Guid RepresentativeId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(ExampleRepresentative), nameof(RepresentativeId), nameof(ExampleRepresentative.Id), Alias = "rep")]
  public ExampleRepresentative? Representative { get; set; }
}

[DbTable("example_representatives")]
public sealed class ExampleRepresentative
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

public sealed class ExampleTeamProjectionByType
{
  [DbColumn("name")]
  [ProjectionSource(typeof(ExampleTeam))]
  public string TeamName { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource(typeof(ExampleRepresentative))]
  public string RepresentativeName { get; set; } = string.Empty;
}
