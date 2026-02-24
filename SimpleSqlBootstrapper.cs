namespace CodeWorks.SimpleSql;

public static class SimpleSqlBootstrapper
{
  public static async Task TrySyncDbAsync(Func<Task> initializeAsync, Action<string>? log = null)
  {
    try
    {
      await initializeAsync();
      (log ?? Console.WriteLine)("DB INITIALIZATION: SUCCESS");
    }
    catch (Exception ex)
    {
      (log ?? Console.WriteLine)("DB INITIALIZATION: FAILED");
      (log ?? Console.WriteLine)(ex.ToString());
      throw;
    }
  }
}
