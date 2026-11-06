using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace S3CompareService.Core.Storage;

public class SqliteConnectionFactory
{
    private readonly string _connectionString;

    public SqliteConnectionFactory(string? connectionString = null)
    {
        _connectionString = connectionString ?? DatabaseConfig.GetConnectionString();
    }

    public async Task<SqliteConnection> CreateOpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        // Ensure WAL mode and reasonable busy timeout
        using (var cmd = conn.CreateCommand())
        {
            // auto_vacuum only takes effect on a database that has no tables yet (a new file); on an existing file the
            // statement is a harmless no-op. Existing databases are converted once by the retention service
            // (RetentionManager.EnsureIncrementalVacuumAsync). It comes before journal_mode, which writes the header.
            // journal_size_limit caps how large the -wal file may stay after a checkpoint.
            cmd.CommandText = @"
                PRAGMA auto_vacuum = INCREMENTAL;
                PRAGMA journal_size_limit = 67108864;
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA busy_timeout = 10000;
            ";
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return conn;
    }
}
