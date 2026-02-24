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

## Quick start

```csharp
SqlHelper.UseDialect(SqlDialects.Postgres);

var sql = SqlHelper.For<MyEntity>();
var insertColumns = sql.InsertColumns;
var insertValues = sql.InsertValues;
```

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
