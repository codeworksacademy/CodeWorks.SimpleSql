using System.Data;
using CodeWorks.SimpleSql;
using Xunit;

namespace CodeWorks.SimpleSql.Tests;

public class DbSetTests
{
  [Fact]
  public void ToCompiledQuery_BuildsWhereOrderAndPagingSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active")
      .OrderBy(x => x.CreatedAt, desc: true)
      .Page(2, 25);

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
    Assert.Contains("ORDER BY t0.\"created_at\" DESC", compiled.Sql);
    Assert.Contains("LIMIT 25 OFFSET 25", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_WithInclude_BuildsRelationshipJoinSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<RepoCustomer>(x => x.Customer)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("LEFT JOIN \"repo_customers\" c1 ON t0.\"customer_id\" = c1.\"id\"", compiled.Sql);
    Assert.Contains("c1.\"name\" AS \"Name\"", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_WithIncludeMissingDbRelation_UnwrapsHelpfulException()
  {
    var dbSet = new DbSet<QueryOrderWithoutRelation>(new FakeConnection(), SqlDialects.Postgres)
      .Include<QueryCustomer>(x => x.Customer);

    var ex = Assert.Throws<InvalidOperationException>(() => dbSet.ToCompiledQuery());
    Assert.Contains("missing [DbRelation]", ex.Message);
  }

  [Fact]
  public void DbSet_IsImmutable_WhenAddingWhereClause()
  {
    var original = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var filtered = original.Where(x => x.Status == "active");

    var originalSql = original.ToCompiledQuery().Sql;
    var filteredSql = filtered.ToCompiledQuery().Sql;

    Assert.DoesNotContain("WHERE", originalSql);
    Assert.Contains("WHERE", filteredSql);
  }

  [Fact]
  public void ToCompiledQuery_FirstMode_AddsSingleRowLimiter()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active")
      .OrderBy(x => x.CreatedAt);

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.First);

    Assert.Contains("ORDER BY t0.\"created_at\" ASC", compiled.Sql);
    Assert.EndsWith("LIMIT 1", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_CountMode_BuildsCountSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.Count);

    Assert.StartsWith("SELECT COUNT(1) FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_ExistsMode_BuildsExistsSql()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Status == "active");

    var compiled = dbSet.ToCompiledQuery(SqlQueryResultMode.Exists);

    Assert.StartsWith("SELECT EXISTS (SELECT 1 FROM \"repo_orders\" t0", compiled.Sql);
    Assert.Contains("WHERE (t0.\"status\" = @p0)", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_Search_ForPostgres_UsesSearchVectorAndRankOrdering()
  {
    var dbSet = new DbSet<PersonSearchEntity>(new FakeConnection(), SqlDialects.Postgres)
      .Search("john doe");

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("t0.\"person_search_vector\" @@", compiled.Sql);
    Assert.Contains("ORDER BY ts_rank(t0.\"person_search_vector\"", compiled.Sql);
    Assert.Equal("john doe", compiled.Parameters.Get<string>("q"));
  }

  [Fact]
  public void ToCompiledQuery_Search_ForPostgresPhraseMode_UsesPhraseTsQuery()
  {
    var dbSet = new DbSet<PersonSearchEntity>(new FakeConnection(), SqlDialects.Postgres)
      .Search("john doe", new SqlSearchOptions { Mode = SqlSearchMode.Phrase });

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("phraseto_tsquery('simple', @q)", compiled.Sql);
    Assert.Contains("ORDER BY ts_rank(t0.\"person_search_vector\"", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_Search_ForSqlServer_UsesSourceFieldFallback()
  {
    var dbSet = new DbSet<PersonSearchEntity>(new FakeConnection(), SqlDialects.SqlServer)
      .Search("john doe");

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("t0.[first_name]", compiled.Sql);
    Assert.Contains("t0.[last_name]", compiled.Sql);
    Assert.DoesNotContain("person_search_vector @@", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_Search_ComposesWithWhereAndProjection()
  {
    var query = new DbSet<PersonSearchEntity>(new FakeConnection(), SqlDialects.Postgres)
      .Search("john doe")
      .Where(x => x.FirstName == "John")
      .Select<PersonSearchProjection>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("t0.\"person_search_vector\" @@", compiled.Sql);
    Assert.Contains("WHERE (t0.\"first_name\" = @p0) AND", compiled.Sql);
    Assert.Contains("ORDER BY ts_rank(t0.\"person_search_vector\"", compiled.Sql);
    Assert.Contains("SELECT t0.\"id\" AS \"Id\", t0.\"first_name\" AS \"FirstName\", t0.\"last_name\" AS \"LastName\"", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_ForPostgres_BuildsOnConflictUpdate()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var row = new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.Contains("INSERT INTO \"repo_orders\"", compiled.Sql);
    Assert.Contains("ON CONFLICT (\"id\")", compiled.Sql);
    Assert.Contains("DO UPDATE SET", compiled.Sql);
    Assert.Contains("\"customer_id\" = EXCLUDED.\"customer_id\"", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_ForSqlServer_BuildsMergeStatement()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.SqlServer);
    var row = new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.StartsWith("MERGE [repo_orders] AS target", compiled.Sql);
    Assert.Contains("ON target.[id] = source.[id]", compiled.Sql);
    Assert.Contains("WHEN NOT MATCHED THEN INSERT", compiled.Sql);
  }

  [Fact]
  public void ToUpsertCompiledQuery_IncludesParametersForAllInsertColumns()
  {
    var id = Guid.NewGuid();
    var customerId = Guid.NewGuid();
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);
    var row = new RepoOrder
    {
      Id = id,
      CustomerId = customerId,
      Status = "active"
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.Equal(id, compiled.Parameters.Get<Guid>("Id"));
    Assert.Equal(customerId, compiled.Parameters.Get<Guid>("CustomerId"));
    Assert.Equal("active", compiled.Parameters.Get<string>("Status"));
  }

  [Fact]
  public void ToUpsertCompiledQuery_ForPostgresVector_CastsAndSerializesEmbedding()
  {
    var dbSet = new DbSet<VectorDocument>(new FakeConnection(), SqlDialects.Postgres);
    var row = new VectorDocument
    {
      Id = Guid.NewGuid(),
      Embedding = [0.1f, 0.2f, 0.3f]
    };

    var compiled = dbSet.ToUpsertCompiledQuery(row, x => x.Id);

    Assert.Contains("@Embedding::VECTOR", compiled.Sql);
    Assert.Equal("[0.1,0.2,0.3]", compiled.Parameters.Get<string>("Embedding"));
  }

  [Fact]
  public void ToUpsertCompiledQuery_UsesStableSqlForDifferentRows()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres);

    var sqlA = dbSet.ToUpsertCompiledQuery(new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "a"
    }, x => x.Id).Sql;

    var sqlB = dbSet.ToUpsertCompiledQuery(new RepoOrder
    {
      Id = Guid.NewGuid(),
      CustomerId = Guid.NewGuid(),
      Status = "b"
    }, x => x.Id).Sql;

    Assert.Equal(sqlA, sqlB);
  }

  [Fact]
  public async Task ToListAsync_Throws_WhenTransactionConnectionDoesNotMatch()
  {
    var db = new FakeConnection();
    var otherDb = new FakeConnection();
    var tx = new FakeTransaction(otherDb);

    var dbSet = new DbSet<RepoOrder>(db, SqlDialects.Postgres)
      .Where(x => x.Status == "active");

    var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => dbSet.ToListAsync(tx));
    Assert.Contains("does not belong to the DbSet connection", ex.Message);
  }

  [Fact]
  public void ToCompiledQuery_ForSqlServerPaging_UsesOffsetFetchAndFallbackOrderBy()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.SqlServer)
      .Where(x => x.Status == "active")
      .Page(1, 10);

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("ORDER BY (SELECT 1) OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", compiled.Sql);
  }

  [Fact]
  public void ToCompiledQuery_ForSqlServerPaging_WithOrderBy_DoesNotUseFallbackOrderBy()
  {
    var dbSet = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.SqlServer)
      .Where(x => x.Status == "active")
      .OrderBy(x => x.CreatedAt)
      .Page(2, 10);

    var compiled = dbSet.ToCompiledQuery();

    Assert.Contains("ORDER BY t0.[created_at] ASC OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", compiled.Sql);
    Assert.DoesNotContain("ORDER BY (SELECT 1)", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_SameTable_LimitsSelectedColumns()
  {
    var query = new DbSet<AccountRecord>(new FakeConnection(), SqlDialects.Postgres)
      .Where(x => x.Active)
      .Select<PublicProfile>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("SELECT t0.\"id\" AS \"Id\", t0.\"display_name\" AS \"DisplayName\"", compiled.Sql);
    Assert.DoesNotContain("email", compiled.Sql, StringComparison.OrdinalIgnoreCase);
    Assert.Contains("FROM \"accounts\" t0", compiled.Sql);
    Assert.Contains("WHERE t0.\"active\"", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_WithInclude_ThrowsNotSupported()
  {
    var query = new DbSet<RepoOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<RepoCustomer>(x => x.Customer)
      .Select<OrderWithCustomerProfile>();

    var compiled = query.ToCompiledQuery();
    Assert.Contains("c1.\"name\" AS \"CustomerName\"", compiled.Sql);
    Assert.Contains("t0.\"status\" AS \"Status\"", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_RespectsIgnoreSelectOnProjectionType()
  {
    var query = new DbSet<AccountRecord>(new FakeConnection(), SqlDialects.Postgres)
      .Select<PublicProfileWithHidden>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("t0.\"id\" AS \"Id\"", compiled.Sql);
    Assert.Contains("t0.\"display_name\" AS \"DisplayName\"", compiled.Sql);
    Assert.DoesNotContain("AS \"Email\"", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_WithMultipleIncludeSources_ThrowsWhenAmbiguousWithoutHint()
  {
    var query = new DbSet<CollisionOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<CollisionCustomer>(x => x.Customer, alias: "cust")
      .Include<CollisionRepresentative>(x => x.Representative, alias: "rep")
      .Select<CollisionProjectionAmbiguous>();

    var ex = Assert.Throws<InvalidOperationException>(() => query.ToCompiledQuery());
    Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
  }

  [Fact]
  public void SelectProjection_WithProjectionSourceAlias_ResolvesColumnCollisions()
  {
    var query = new DbSet<CollisionOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<CollisionCustomer>(x => x.Customer, alias: "cust")
      .Include<CollisionRepresentative>(x => x.Representative, alias: "rep")
      .Select<CollisionProjectionResolved>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("cust.\"name\" AS \"CustomerName\"", compiled.Sql);
    Assert.Contains("rep.\"name\" AS \"RepresentativeName\"", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_WithProjectionSourceType_ResolvesRootVsIncludeCollision()
  {
    var query = new DbSet<TypeCollisionOrder>(new FakeConnection(), SqlDialects.Postgres)
      .Include<TypeCollisionCustomer>(x => x.Customer, alias: "cust")
      .Select<TypeCollisionProjectionByType>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("t0.\"name\" AS \"OrderName\"", compiled.Sql);
    Assert.Contains("cust.\"name\" AS \"CustomerName\"", compiled.Sql);
  }

  [Fact]
  public void SelectProjection_EmployeeRecord_WithBusinessAndAccount_IncludesRequestedColumns()
  {
    var accountId = Guid.NewGuid();

    var query = new DbSet<EmployeeRecord>(new FakeConnection(), SqlDialects.Postgres)
      .Where(e => e.AccountId == accountId)
      .Include<EmployeeBusiness>(e => e.Business, alias: "biz")
      .Include<EmployeeAccount>(e => e.Account, alias: "acct")
      .Select<EmployeeRecordProjection>();

    var compiled = query.ToCompiledQuery();

    Assert.Contains("LEFT JOIN \"employee_businesses\" biz ON t0.\"business_id\" = biz.\"id\"", compiled.Sql);
    Assert.Contains("LEFT JOIN \"employee_accounts\" acct ON t0.\"account_id\" = acct.\"id\"", compiled.Sql);
    Assert.Contains("biz.\"name\" AS \"BusinessName\"", compiled.Sql);
    Assert.Contains("biz.\"logo\" AS \"BusinessLogo\"", compiled.Sql);
    Assert.Contains("acct.\"name\" AS \"AccountName\"", compiled.Sql);
    Assert.Contains("WHERE (t0.\"account_id\" = @p0)", compiled.Sql);
  }
}

[DbTable("accounts")]
public sealed class AccountRecord
{
  public Guid Id { get; set; }

  [DbColumn("display_name")]
  public string DisplayName { get; set; } = string.Empty;

  public string Email { get; set; } = string.Empty;
  public bool Active { get; set; }
}

[DbTable("accounts")]
public sealed class PublicProfile
{
  public Guid Id { get; set; }

  [DbColumn("display_name")]
  public string DisplayName { get; set; } = string.Empty;
}

[DbTable("accounts")]
public sealed class PublicProfileWithHidden
{
  public Guid Id { get; set; }

  [DbColumn("display_name")]
  public string DisplayName { get; set; } = string.Empty;

  [IgnoreSelect]
  public string Email { get; set; } = string.Empty;
}

public sealed class OrderWithCustomerProfile
{
  public string Status { get; set; } = string.Empty;

  [DbColumn("name")]
  public string CustomerName { get; set; } = string.Empty;
}

public sealed class PersonSearchProjection
{
  public Guid Id { get; set; }

  [DbColumn("first_name")]
  public string FirstName { get; set; } = string.Empty;

  [DbColumn("last_name")]
  public string LastName { get; set; } = string.Empty;
}

[DbTable("collision_orders")]
public sealed class CollisionOrder
{
  public Guid Id { get; set; }

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  [DbColumn("representative_id")]
  public Guid RepresentativeId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(CollisionCustomer), nameof(CustomerId), nameof(CollisionCustomer.Id), Alias = "cust")]
  public CollisionCustomer? Customer { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(CollisionRepresentative), nameof(RepresentativeId), nameof(CollisionRepresentative.Id), Alias = "rep")]
  public CollisionRepresentative? Representative { get; set; }
}

[DbTable("collision_customers")]
public sealed class CollisionCustomer
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

[DbTable("collision_representatives")]
public sealed class CollisionRepresentative
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

public sealed class CollisionProjectionAmbiguous
{
  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

public sealed class CollisionProjectionResolved
{
  [DbColumn("name")]
  [ProjectionSource("cust")]
  public string CustomerName { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource("rep")]
  public string RepresentativeName { get; set; } = string.Empty;
}

[DbTable("type_collision_orders")]
public sealed class TypeCollisionOrder
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(TypeCollisionCustomer), nameof(CustomerId), nameof(TypeCollisionCustomer.Id), Alias = "cust")]
  public TypeCollisionCustomer? Customer { get; set; }
}

[DbTable("type_collision_customers")]
public sealed class TypeCollisionCustomer
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

public sealed class TypeCollisionProjectionByType
{
  [DbColumn("name")]
  [ProjectionSource(typeof(TypeCollisionOrder))]
  public string OrderName { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource(typeof(TypeCollisionCustomer))]
  public string CustomerName { get; set; } = string.Empty;
}

[DbTable("employee_records")]
public sealed class EmployeeRecord
{
  public Guid Id { get; set; }

  [DbColumn("account_id")]
  public Guid AccountId { get; set; }

  [DbColumn("business_id")]
  public Guid BusinessId { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(EmployeeBusiness), nameof(BusinessId), nameof(EmployeeBusiness.Id), Alias = "biz")]
  public EmployeeBusiness? Business { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(EmployeeAccount), nameof(AccountId), nameof(EmployeeAccount.Id), Alias = "acct")]
  public EmployeeAccount? Account { get; set; }
}

[DbTable("employee_businesses")]
public sealed class EmployeeBusiness
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;

  [DbColumn("logo")]
  public string Logo { get; set; } = string.Empty;
}

[DbTable("employee_accounts")]
public sealed class EmployeeAccount
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  public string Name { get; set; } = string.Empty;
}

public sealed class EmployeeRecordProjection
{
  public Guid Id { get; set; }

  [DbColumn("name")]
  [ProjectionSource(typeof(EmployeeBusiness))]
  public string BusinessName { get; set; } = string.Empty;

  [DbColumn("logo")]
  [ProjectionSource(typeof(EmployeeBusiness))]
  public string BusinessLogo { get; set; } = string.Empty;

  [DbColumn("name")]
  [ProjectionSource(typeof(EmployeeAccount))]
  public string AccountName { get; set; } = string.Empty;
}

[DbTable("repo_orders")]
public sealed class RepoOrder
{
  public Guid Id { get; set; }

  [DbColumn("customer_id")]
  public Guid CustomerId { get; set; }

  public string Status { get; set; } = string.Empty;
  public DateTime CreatedAt { get; set; }

  [IgnoreWrite]
  [IgnoreSelect]
  [DbRelation(typeof(RepoCustomer), nameof(CustomerId), nameof(RepoCustomer.Id), Alias = "c1")]
  public RepoCustomer? Customer { get; set; }
}

[DbTable("repo_customers")]
public sealed class RepoCustomer
{
  public Guid Id { get; set; }
  public string Name { get; set; } = string.Empty;
}

[DbTable("vector_documents")]
public sealed class VectorDocument
{
  public Guid Id { get; set; }

  [DbVector(3)]
  public float[] Embedding { get; set; } = [];
}

internal sealed class FakeConnection : IDbConnection
{
  public string ConnectionString { get; set; } = string.Empty;
  public int ConnectionTimeout => 0;
  public string Database => string.Empty;
  public ConnectionState State => ConnectionState.Closed;

  public IDbTransaction BeginTransaction() => throw new NotSupportedException();
  public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();
  public void ChangeDatabase(string databaseName) => throw new NotSupportedException();
  public void Close() { }
  public IDbCommand CreateCommand() => throw new NotSupportedException();
  public void Open() { }
  public void Dispose() { }
}

internal sealed class FakeTransaction(IDbConnection connection) : IDbTransaction
{
  public IDbConnection Connection => connection;
  public IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
  public void Commit() { }
  public void Rollback() { }
  public void Dispose() { }
}
