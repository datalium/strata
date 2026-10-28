using System.Globalization;
using Npgsql;

namespace Datalium.Strata.Storage.Postgres;

/// <summary>
/// Connection-string handling for the Postgres stores. They accept a native Npgsql connection string
/// (<c>Host=localhost;Username=strata;…</c>) or the <c>postgres://user:password@host:port/database</c>
/// URL the Rust adapters take, so one <c>STRATA_TEST_POSTGRES_URL</c> works for both runtimes.
/// </summary>
public static class PostgresConnection
{
    /// <summary>
    /// Returns <paramref name="urlOrConnectionString"/> as an Npgsql connection string, converting a
    /// <c>postgres://</c> or <c>postgresql://</c> URL and passing anything else through untouched. A
    /// <c>sslmode</c> query parameter is honoured; other query parameters are rejected rather than
    /// silently ignored.
    /// </summary>
    public static string ToConnectionString(string urlOrConnectionString)
    {
        if (!urlOrConnectionString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase)
            && !urlOrConnectionString.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
        {
            return urlOrConnectionString;
        }

        var uri = new Uri(urlOrConnectionString);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = uri.Host,
            Timeout = 5,
        };
        if (!uri.IsDefaultPort)
        {
            builder.Port = uri.Port;
        }

        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var separator = uri.UserInfo.IndexOf(':');
            builder.Username = Uri.UnescapeDataString(separator < 0 ? uri.UserInfo : uri.UserInfo[..separator]);
            if (separator >= 0)
            {
                builder.Password = Uri.UnescapeDataString(uri.UserInfo[(separator + 1)..]);
            }
        }

        var database = uri.AbsolutePath.Trim('/');
        if (database.Length > 0)
        {
            builder.Database = Uri.UnescapeDataString(database);
        }

        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            var name = Uri.UnescapeDataString(parts[0]);
            var value = parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : string.Empty;
            if (!name.Equals("sslmode", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(
                    $"Unsupported query parameter '{name}' in the postgres URL; use an Npgsql connection string instead.",
                    nameof(urlOrConnectionString));
            }

            builder.SslMode = Enum.Parse<SslMode>(value.Replace("-", string.Empty), ignoreCase: true);
        }

        return builder.ConnectionString;
    }

    internal static NpgsqlDataSource CreateDataSource(string urlOrConnectionString) =>
        NpgsqlDataSource.Create(ToConnectionString(urlOrConnectionString));
}
