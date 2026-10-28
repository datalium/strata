using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Datalium.Strata.Storage;

/// <summary>
/// Value offload (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"): a store with these options stores each
/// top-level value of a level's state whose JSON is at least <see cref="Threshold"/> bytes once, in a blob store,
/// and refers to it from the row. Mirrors <c>strata-memory</c>'s <c>Offload</c>.
/// </summary>
public sealed record OffloadOptions
{
    /// <summary>The bytes of a value's JSON from which it is offloaded. At least 1.</summary>
    public int Threshold { get; init; } = 64 * 1024;

    /// <summary>
    /// Where offloaded values go, and where the store reads them from. <see langword="null"/>, the default: the
    /// checkpoint store's own database (a <c>checkpoint_blobs</c> table in SQL, a hash per thread in Redis).
    /// </summary>
    public ICheckpointBlobStore? Blobs { get; init; }

    internal OffloadOptions Checked()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(Threshold, 1, nameof(Threshold));
        return this;
    }
}

/// <summary>
/// Where offloaded values live (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"): content addressed within a
/// thread, by <c>sha256:</c> and the lowercase hex SHA-256 of the content. A checkpoint store calls it; the engine
/// never does. S3 and the like are the application's, through this port. Mirrors <c>strata-memory</c>'s
/// <c>BlobStore</c>.
/// </summary>
public interface ICheckpointBlobStore
{
    /// <summary>Stores <paramref name="content"/> under (<paramref name="threadId"/>, <paramref name="hash"/>). Storing what is already there is a no-op.</summary>
    ValueTask PutAsync(string threadId, string hash, ReadOnlyMemory<byte> content, CancellationToken cancellationToken = default);

    /// <summary>The content stored under (<paramref name="threadId"/>, <paramref name="hash"/>), or <see langword="null"/>.</summary>
    ValueTask<ReadOnlyMemory<byte>?> GetAsync(string threadId, string hash, CancellationToken cancellationToken = default);

    /// <summary>Deletes every blob of <paramref name="threadId"/>: what a purge calls.</summary>
    ValueTask DeleteThreadAsync(string threadId, CancellationToken cancellationToken = default);
}

/// <summary>Blobs in a dictionary: for tests, and for a store in memory. Thread-safe.</summary>
public sealed class InMemoryCheckpointBlobStore : ICheckpointBlobStore
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte[]>> _threads = new(StringComparer.Ordinal);

    public ValueTask PutAsync(string threadId, string hash, ReadOnlyMemory<byte> content, CancellationToken cancellationToken = default)
    {
        _threads.GetOrAdd(threadId, _ => new ConcurrentDictionary<string, byte[]>(StringComparer.Ordinal))
            .TryAdd(hash, content.ToArray());
        return ValueTask.CompletedTask;
    }

    public ValueTask<ReadOnlyMemory<byte>?> GetAsync(string threadId, string hash, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<ReadOnlyMemory<byte>?>(
            _threads.TryGetValue(threadId, out var blobs) && blobs.TryGetValue(hash, out var content) ? content : null);

    public ValueTask DeleteThreadAsync(string threadId, CancellationToken cancellationToken = default)
    {
        _threads.TryRemove(threadId, out _);
        return ValueTask.CompletedTask;
    }

    /// <summary>How many blobs <paramref name="threadId"/> has.</summary>
    public int Count(string threadId) => _threads.TryGetValue(threadId, out var blobs) ? blobs.Count : 0;
}

/// <summary>
/// Blobs as files: <c>&lt;root&gt;/&lt;thread id as lowercase hex of its UTF-8&gt;/&lt;hex of the hash&gt;</c>, the
/// hash's hex digest without its <c>sha256:</c>. A file is written under a temporary name and renamed into place,
/// so a reader never sees half of one. Mirrors <c>strata-memory</c>'s <c>FileSystemBlobStore</c>.
/// </summary>
public sealed class FileSystemCheckpointBlobStore(string root) : ICheckpointBlobStore
{
    private const string Prefix = "sha256:";

    /// <summary>The directory the store keeps its threads under.</summary>
    public string Root { get; } = root ?? throw new ArgumentNullException(nameof(root));

    /// <summary>The directory of <paramref name="threadId"/>'s blobs.</summary>
    public string ThreadDirectory(string threadId) =>
        Path.Combine(Root, Convert.ToHexStringLower(Encoding.UTF8.GetBytes(threadId)));

    private string FilePath(string threadId, string hash) =>
        hash.StartsWith(Prefix, StringComparison.Ordinal) && hash.Length > Prefix.Length
            && !hash.AsSpan(Prefix.Length).ContainsAnyExcept("0123456789abcdef")
            ? Path.Combine(ThreadDirectory(threadId), hash[Prefix.Length..])
            : throw new ArgumentException($"'{hash}' is not a sha256: hash", nameof(hash));

    public async ValueTask PutAsync(string threadId, string hash, ReadOnlyMemory<byte> content, CancellationToken cancellationToken = default)
    {
        var path = FilePath(threadId, hash);
        if (File.Exists(path))
        {
            return;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var temporary = $"{path}.{Guid.NewGuid():N}.tmp";
        await File.WriteAllBytesAsync(temporary, content, cancellationToken).ConfigureAwait(false);
        try
        {
            File.Move(temporary, path);
        }
        catch (IOException) when (File.Exists(path))
        {
            // Content addressed: whoever renamed first wrote what this would have.
        }
        finally
        {
            File.Delete(temporary);
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>?> GetAsync(string threadId, string hash, CancellationToken cancellationToken = default)
    {
        try
        {
            return await File.ReadAllBytesAsync(FilePath(threadId, hash), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
    }

    public ValueTask DeleteThreadAsync(string threadId, CancellationToken cancellationToken = default)
    {
        try
        {
            Directory.Delete(ThreadDirectory(threadId), recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
            // Nothing stored for the thread.
        }

        return ValueTask.CompletedTask;
    }
}

/// <summary>A value a save stores in a blob store: its hash and its JSON text.</summary>
internal sealed record CheckpointBlob(string Hash, ReadOnlyMemory<byte> Content);

/// <summary>Hashes of offloaded values, and how a store reads them back.</summary>
internal static class OffloadedBlobs
{
    /// <summary>Whether <paramref name="content"/> is what <paramref name="hash"/> names.</summary>
    public static bool Matches(string hash, ReadOnlySpan<byte> content)
    {
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(content, digest);
        return hash == "sha256:" + Convert.ToHexStringLower(digest);
    }

    /// <summary>The contents <paramref name="hashes"/> name in <paramref name="store"/>, those it has.</summary>
    public static async ValueTask<IReadOnlyDictionary<string, ReadOnlyMemory<byte>>> ReadAsync(
        ICheckpointBlobStore store, string threadId, IReadOnlyCollection<string> hashes, CancellationToken cancellationToken)
    {
        var found = new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal);
        foreach (var hash in hashes)
        {
            if (await store.GetAsync(threadId, hash, cancellationToken).ConfigureAwait(false) is { } content)
            {
                found[hash] = content;
            }
        }

        return found;
    }

    /// <summary>Writes <paramref name="blobs"/> to <paramref name="store"/>, before the row that refers to them.</summary>
    public static async ValueTask PutAsync(
        ICheckpointBlobStore store, string threadId, IReadOnlyList<CheckpointBlob> blobs, CancellationToken cancellationToken)
    {
        foreach (var blob in blobs)
        {
            await store.PutAsync(threadId, blob.Hash, blob.Content, cancellationToken).ConfigureAwait(false);
        }
    }
}
