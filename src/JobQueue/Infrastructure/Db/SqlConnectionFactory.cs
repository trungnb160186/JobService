using System.Data;
using Microsoft.Data.SqlClient;

namespace AutoMealAllocation.Infrastructure.Db;

public interface IDbConnectionFactory
{
    Task<IDbConnection> CreateOpenConnectionAsync(CancellationToken ct);
}

public sealed class SqlConnectionFactory(IConfiguration cfg) : IDbConnectionFactory
{
    private readonly string _cs = cfg.GetConnectionString("SqlConnection") ?? throw new InvalidOperationException("Connection string 'SqlConnection' not found.");

    public async Task<IDbConnection> CreateOpenConnectionAsync(CancellationToken ct)
    {
        var con = new SqlConnection(_cs);
        await con.OpenAsync(ct);
        return con;
    }
}
