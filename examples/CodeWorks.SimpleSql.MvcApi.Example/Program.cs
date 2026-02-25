using CodeWorks.SimpleSql;
using CodeWorks.SimpleSql.MvcApi.Example.Repositories;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
var port = Environment.GetEnvironmentVariable("PORT") ?? "5175";
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(int.Parse(port));
});


builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Configuration.AddJsonFile(Path.Combine(AppContext.BaseDirectory, "appsettings.json"), optional: true, reloadOnChange: true)
  .AddEnvironmentVariables();

var connectionString = builder.Configuration.GetConnectionString("Default")
  ?? Environment.GetEnvironmentVariable("SIMPLESQL_EXAMPLE_CONNECTION")
  ?? "Host=localhost;Database=TestDb;Username=postgres;Password=localdb;";

builder.Services.AddSingleton(new NpgsqlDataSourceBuilder(connectionString).Build());
builder.Services.AddScoped<IAccountsRepository, AccountsRepository>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.Use((context, next) =>
{
    Console.WriteLine($"[{DateTime.UtcNow:O}] {context.Request.Method} {context.Request.Path}");
    return next(context);
});


app.MapControllers();


await EnsureSchemaAsync(app.Services);

app.Run();

static async Task EnsureSchemaAsync(IServiceProvider services)
{
    using var scope = services.CreateScope();
    var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();

    await using var db = await dataSource.OpenConnectionAsync();
    await using var tx = await db.BeginTransactionAsync();

    await SchemaSync.SyncModelsAsync(
      db,
      tx,
      [
        typeof(CodeWorks.SimpleSql.MvcApi.Example.Repositories.Account),
      typeof(CodeWorks.SimpleSql.MvcApi.Example.Repositories.User)
      ],
      options: new SchemaSyncOptions
      {
          LogPath = Path.Combine(AppContext.BaseDirectory, "logs", "db-sync.log"),
          EnableConsoleLogging = true
      });

    await tx.CommitAsync();
}
