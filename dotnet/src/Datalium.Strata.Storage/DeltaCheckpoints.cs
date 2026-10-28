using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage;

/// <summary>
/// Delta checkpoints (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas"): a store with these options stores a
/// <c>running</c> checkpoint as the top-level keys of each level's state that changed since the thread's previous
/// row, when it knows that row, and every other checkpoint whole. Mirrors <c>strata-memory</c>'s <c>Deltas</c>.
/// </summary>
public sealed record DeltaOptions
{
    /// <summary>
    /// Every this many rows of a thread one is stored whole, so a load reads at most this many. At least 2.
    /// </summary>
    public int FullEvery { get; init; } = 16;

    /// <summary>
    /// How many threads' latest rows the store remembers (their fingerprints, about 40 bytes a key), the least
    /// recently used forgotten first. A thread the store doesn't remember is saved whole. At least 1.
    /// </summary>
    public int CachedThreads { get; init; } = 10_000;

    internal DeltaOptions Checked()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(FullEvery, 2, nameof(FullEvery));
        ArgumentOutOfRangeException.ThrowIfLessThan(CachedThreads, 1, nameof(CachedThreads));
        return this;
    }
}

/// <summary>
/// The SHA-256 of a value's JSON text, with object keys in ordinal order. It is also the hash an offloaded value
/// is stored under (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"): the blob is that same text.
/// </summary>
internal readonly record struct Fingerprint(ulong A, ulong B, ulong C, ulong D)
{
    /// <summary>Compact, and without escaping what JSON doesn't need escaped, as <c>serde_json</c> writes it.</summary>
    private static readonly JsonWriterOptions Canonical = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static Fingerprint Of(JsonNode? value) => Of(value, int.MaxValue, out _);

    /// <summary>
    /// The fingerprint of <paramref name="value"/>, and, when its text is at least <paramref name="keepFrom"/> bytes
    /// long, that text: what an offloaded value is stored as.
    /// </summary>
    public static Fingerprint Of(JsonNode? value, int keepFrom, out byte[]? text)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, Canonical))
        {
            WriteCanonical(writer, value);
        }

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(buffer.WrittenSpan, hash);
        text = buffer.WrittenCount >= keepFrom ? buffer.WrittenSpan.ToArray() : null;
        return new Fingerprint(
            BinaryPrimitives.ReadUInt64LittleEndian(hash),
            BinaryPrimitives.ReadUInt64LittleEndian(hash[8..]),
            BinaryPrimitives.ReadUInt64LittleEndian(hash[16..]),
            BinaryPrimitives.ReadUInt64LittleEndian(hash[24..]));
    }

    /// <summary>The fingerprint a blob's <c>sha256:</c> hash names: an offloaded value's, without printing it again.</summary>
    public static Fingerprint FromHash(string hash)
    {
        Span<byte> digest = stackalloc byte[32];
        Convert.FromHexString(hash.AsSpan("sha256:".Length), digest, out _, out _);
        return new Fingerprint(
            BinaryPrimitives.ReadUInt64LittleEndian(digest),
            BinaryPrimitives.ReadUInt64LittleEndian(digest[8..]),
            BinaryPrimitives.ReadUInt64LittleEndian(digest[16..]),
            BinaryPrimitives.ReadUInt64LittleEndian(digest[24..]));
    }

    /// <summary><c>sha256:</c> and the lowercase hex digest: the name of the blob a value is offloaded as.</summary>
    public string Hash
    {
        get
        {
            Span<byte> digest = stackalloc byte[32];
            BinaryPrimitives.WriteUInt64LittleEndian(digest, A);
            BinaryPrimitives.WriteUInt64LittleEndian(digest[8..], B);
            BinaryPrimitives.WriteUInt64LittleEndian(digest[16..], C);
            BinaryPrimitives.WriteUInt64LittleEndian(digest[24..], D);
            return "sha256:" + Convert.ToHexStringLower(digest);
        }
    }

    /// <summary>
    /// Keys sorted, so a value read back from a <c>JSONB</c> column, which reorders them, prints as it was saved.
    /// A value that still prints differently (a number written another way) only makes a delta bigger.
    /// </summary>
    private static void WriteCanonical(Utf8JsonWriter writer, JsonNode? node)
    {
        switch (node)
        {
            case null:
                writer.WriteNullValue();
                break;
            case JsonObject json:
                writer.WriteStartObject();
                foreach (var (key, value) in json.OrderBy(property => property.Key, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(key);
                    WriteCanonical(writer, value);
                }

                writer.WriteEndObject();
                break;
            case JsonArray array:
                writer.WriteStartArray();
                foreach (var item in array)
                {
                    WriteCanonical(writer, item);
                }

                writer.WriteEndArray();
                break;
            default:
                node.WriteTo(writer);
                break;
        }
    }
}

/// <summary>
/// The fingerprints of each level's state, outermost first: a key's fingerprint per top-level key, or
/// <see langword="null"/> for a level whose state isn't a JSON object.
/// </summary>
internal sealed record LevelPrints(IReadOnlyList<Dictionary<string, Fingerprint>?> Levels)
{
    public static LevelPrints Of(Checkpoint checkpoint) => Of(checkpoint, null, out _);

    /// <summary>
    /// The fingerprints of a decoded <paramref name="checkpoint"/>, its offloaded values' (<paramref name="slots"/>)
    /// taken from their hashes, which are those fingerprints, rather than printed and hashed again.
    /// </summary>
    public static LevelPrints Of(Checkpoint checkpoint, IReadOnlyList<BlobSlot> slots)
    {
        if (slots.Count == 0)
        {
            return Of(checkpoint);
        }

        var levels = CheckpointEncoding.Levels(checkpoint);
        var prints = new List<Dictionary<string, Fingerprint>?>(levels.Count);
        for (var i = 0; i < levels.Count; i++)
        {
            if (levels[i].FrozenState is not JsonObject state)
            {
                prints.Add(null);
                continue;
            }

            var known = slots.Where(slot => slot.Level == i).ToDictionary(slot => slot.Key, slot => slot.Hash, StringComparer.Ordinal);
            prints.Add(state.ToDictionary(
                property => property.Key,
                property => known.TryGetValue(property.Key, out var hash) ? Fingerprint.FromHash(hash) : Fingerprint.Of(property.Value),
                StringComparer.Ordinal));
        }

        return new LevelPrints(prints);
    }

    /// <summary>
    /// The fingerprints of <paramref name="checkpoint"/>, and with an <paramref name="offloadFrom"/>, by level and key,
    /// the JSON text of each top-level value at least that many bytes long.
    /// </summary>
    public static LevelPrints Of(Checkpoint checkpoint, int? offloadFrom, out List<Dictionary<string, byte[]>?>? large)
    {
        var levels = CheckpointEncoding.Levels(checkpoint);
        var prints = new List<Dictionary<string, Fingerprint>?>(levels.Count);
        large = offloadFrom is null ? null : new List<Dictionary<string, byte[]>?>(levels.Count);
        foreach (var level in levels)
        {
            if (level.FrozenState is not JsonObject state)
            {
                prints.Add(null);
                large?.Add(null);
                continue;
            }

            var keys = new Dictionary<string, Fingerprint>(state.Count, StringComparer.Ordinal);
            Dictionary<string, byte[]>? texts = null;
            foreach (var (key, value) in state)
            {
                keys[key] = Fingerprint.Of(value, offloadFrom ?? int.MaxValue, out var text);
                if (text is not null)
                {
                    (texts ??= new Dictionary<string, byte[]>(StringComparer.Ordinal))[key] = text;
                }
            }

            prints.Add(keys);
            large?.Add(texts);
        }

        return new LevelPrints(prints);
    }
}

/// <summary>
/// A thread's latest row as a store remembers it: its sequence, how far it is from a whole row, its fingerprints,
/// the token the store checks it by (the row's <c>created_at</c> in SQL, the SHA-1 of its element in Redis), and
/// the offloaded values it refers to, which the thread's blob store therefore has.
/// </summary>
internal sealed record CachedRow(long Sequence, int Depth, LevelPrints Prints, string Token, IReadOnlySet<string> Blobs);

/// <summary>
/// A checkpoint as a store is about to write it: <see cref="Stored"/> is <see cref="Checkpoint"/> itself, or its
/// encoded form, with its levels' states as the changes to the row at <see cref="DeltaOf"/>, its large values in
/// the blob store, or both.
/// </summary>
internal sealed class EncodedSave
{
    public required Checkpoint Checkpoint { get; init; }

    public required Checkpoint Stored { get; init; }

    /// <summary>The row this one is a delta of, the one just before it.</summary>
    public long? DeltaOf { get; init; }

    public int Depth { get; init; }

    public LevelPrints? Prints { get; init; }

    /// <summary>
    /// The row this save relies on still being the thread's latest, when it is a delta of it or leaves out blobs
    /// the store remembers the thread has: the store appends it only if that row, by sequence and token, is still
    /// the latest, and otherwise writes <see cref="Whole"/>.
    /// </summary>
    public CachedRow? Base { get; init; }

    /// <summary>The values this save writes to the blob store.</summary>
    public IReadOnlyList<CheckpointBlob> Blobs { get; init; } = [];

    /// <summary>The blobs the store can count on the thread having once this row is written.</summary>
    public IReadOnlySet<string> Remembered { get; init; } = new HashSet<string>();

    /// <summary>Whether the row refers to offloaded values.</summary>
    public bool Offloaded { get; init; }

    internal Func<EncodedSave>? WholeForm { get; init; }

    public bool IsDelta => DeltaOf is not null;

    /// <summary>The <c>strata.store.encoding</c> it is recorded under.</summary>
    public string Encoding => (IsDelta, Offloaded) switch
    {
        (true, true) => "delta_offloaded",
        (true, false) => "delta",
        (false, true) => "offloaded",
        _ => "plain",
    };

    public static EncodedSave Plain(Checkpoint checkpoint, LevelPrints? prints = null) =>
        new() { Checkpoint = checkpoint, Stored = checkpoint, Prints = prints };

    /// <summary>
    /// The checkpoint stored without relying on the row before it: what a store writes instead when that row is no
    /// longer the thread's latest. No delta, and every value it offloads written again.
    /// </summary>
    public EncodedSave Whole() => Base is null || WholeForm is null ? this : WholeForm();
}

/// <summary>Where a decoded state's offloaded value goes: the level, the key, the blob, and the row that refers to it.</summary>
internal readonly record struct BlobSlot(int Level, string Key, string Hash, long Sequence);

/// <summary>A row decoded: its checkpoint, and the offloaded values it refers to (already in place once resolved).</summary>
internal sealed record DecodedRow(long Sequence, Checkpoint Checkpoint, IReadOnlyList<BlobSlot> Slots);

/// <summary>The contents a blob store has of <paramref name="hashes"/>, for one thread.</summary>
internal delegate ValueTask<IReadOnlyDictionary<string, ReadOnlyMemory<byte>>> BlobReader(
    IReadOnlyCollection<string> hashes, CancellationToken cancellationToken);

/// <summary>
/// The encoded form of <c>design/41-Checkpoint-Efficiency.md</c> ("Encoded checkpoints"): a checkpoint with
/// <see cref="DeltaVersion"/> or <see cref="OffloadVersion"/>, whose levels' <c>frozen_state</c> are envelopes. Only
/// a store writes it and only a store reads it. Mirrors <c>strata-memory</c>'s <c>deltas</c> module.
/// </summary>
internal static class CheckpointEncoding
{
    /// <summary>The <c>schema_version</c> of a checkpoint stored with a delta. Never a checkpoint the engine sees.</summary>
    public const string DeltaVersion = "1.8.0";

    /// <summary>The <c>schema_version</c> of a checkpoint stored with offloaded values, a delta or not.</summary>
    public const string OffloadVersion = "1.9.0";

    /// <summary>The encoded versions, as a SQL list.</summary>
    public const string EncodedVersionsSql = $"'{DeltaVersion}', '{OffloadVersion}'";

    public static bool IsEncoded(string schemaVersion) => schemaVersion is DeltaVersion or OffloadVersion;

    /// <summary>The levels of <paramref name="checkpoint"/>, outermost first.</summary>
    public static List<Checkpoint> Levels(Checkpoint checkpoint)
    {
        var levels = new List<Checkpoint>();
        for (var level = checkpoint; level is not null; level = level.Nested)
        {
            levels.Add(level);
        }

        return levels;
    }

    /// <summary><paramref name="checkpoint"/> with each level's state replaced, outermost first.</summary>
    private static Checkpoint WithStates(Checkpoint checkpoint, IReadOnlyList<JsonNode?> states, int index = 0) =>
        checkpoint with
        {
            FrozenState = states[index],
            Nested = checkpoint.Nested is null ? null : WithStates(checkpoint.Nested, states, index + 1),
        };

    /// <summary>
    /// <paramref name="checkpoint"/> as a store writes it. With <paramref name="canDelta"/>, each level whose state
    /// and <paramref name="cached"/>'s same level are objects is the keys that changed or are new (<c>set</c>) and
    /// the keys that are gone (<c>unset</c>); any other level is whole. Each top-level value in
    /// <paramref name="large"/> that the row carries (all of a whole level's, a delta level's changed ones) goes to
    /// the blob store instead and is named in the level's <c>blobs</c>; one the thread's blob store has, by
    /// <paramref name="cached"/>, isn't written again unless <paramref name="writeAllBlobs"/>. Stored as it is when no
    /// level is a delta and nothing is offloaded.
    /// </summary>
    public static EncodedSave Encode(
        Checkpoint checkpoint,
        LevelPrints prints,
        IReadOnlyList<Dictionary<string, byte[]>?>? large,
        CachedRow? cached,
        bool canDelta,
        bool writeAllBlobs = false)
    {
        var levels = Levels(checkpoint);
        var envelopes = new List<JsonObject>(levels.Count);
        var anyDelta = false;
        var omitted = false;
        var referred = new HashSet<string>(StringComparer.Ordinal);
        var written = new List<CheckpointBlob>();
        var largeHashes = new HashSet<string>(StringComparer.Ordinal);

        // The reference to level i's value at key, when it is offloaded; the blob is written unless the store
        // remembers the thread has it.
        JsonObject? Reference(int level, string key)
        {
            if (large?[level] is not { } texts || !texts.TryGetValue(key, out var text))
            {
                return null;
            }

            var hash = prints.Levels[level]![key].Hash;
            if (referred.Add(hash))
            {
                if (!writeAllBlobs && cached?.Blobs.Contains(hash) == true)
                {
                    omitted = true;
                }
                else
                {
                    written.Add(new CheckpointBlob(hash, text));
                }
            }

            return new JsonObject { ["hash"] = hash, ["bytes"] = text.Length };
        }

        for (var i = 0; i < levels.Count; i++)
        {
            var envelope = new JsonObject();
            if (levels[i].FrozenState is JsonObject state && prints.Levels[i] is { } now)
            {
                if (large?[i] is { } texts)
                {
                    largeHashes.UnionWith(texts.Keys.Select(key => now[key].Hash));
                }

                var blobs = new JsonObject();
                if (canDelta
                    && i < cached!.Prints.Levels.Count
                    && cached.Prints.Levels[i] is { } before)
                {
                    var set = new JsonObject();
                    foreach (var (key, value) in state)
                    {
                        if (!before.TryGetValue(key, out var print) || print != now[key])
                        {
                            if (Reference(i, key) is { } reference)
                            {
                                blobs[key] = reference;
                            }
                            else
                            {
                                set[key] = value?.DeepClone();
                            }
                        }
                    }

                    var unset = new JsonArray();
                    foreach (var key in before.Keys.Order(StringComparer.Ordinal))
                    {
                        if (!state.ContainsKey(key))
                        {
                            unset.Add(key);
                        }
                    }

                    envelope["set"] = set;
                    envelope["unset"] = unset;
                    anyDelta = true;
                }
                else
                {
                    var whole = new JsonObject();
                    foreach (var (key, value) in state)
                    {
                        if (Reference(i, key) is { } reference)
                        {
                            blobs[key] = reference;
                        }
                        else
                        {
                            whole[key] = value?.DeepClone();
                        }
                    }

                    envelope["state"] = whole;
                }

                if (blobs.Count > 0)
                {
                    envelope["blobs"] = blobs;
                }
            }
            else
            {
                envelope["state"] = levels[i].FrozenState?.DeepClone();
            }

            envelopes.Add(envelope);
        }

        if (!anyDelta && referred.Count == 0)
        {
            return EncodedSave.Plain(checkpoint, prints);
        }

        var depth = anyDelta ? cached!.Depth + 1 : 0;
        var outer = new JsonObject
        {
            ["checkpoint_version"] = checkpoint.SchemaVersion,
            ["delta_of"] = anyDelta ? cached!.Sequence : null,
            ["depth"] = depth,
        };
        foreach (var key in envelopes[0].Select(property => property.Key).ToList())
        {
            var value = envelopes[0][key];
            envelopes[0].Remove(key);
            outer[key] = value;
        }

        envelopes[0] = outer;

        // A row that relies on the one before (a delta of it, or leaving out blobs it says the thread has) keeps
        // what that row's blobs were for the values it still has; any other row, what it refers to itself.
        var based = anyDelta || omitted;
        var remembered = new HashSet<string>(referred, StringComparer.Ordinal);
        if (based)
        {
            remembered.UnionWith(cached!.Blobs.Where(largeHashes.Contains));
        }

        return new EncodedSave
        {
            Checkpoint = checkpoint,
            Stored = WithStates(checkpoint, envelopes) with { SchemaVersion = referred.Count > 0 ? OffloadVersion : DeltaVersion },
            DeltaOf = anyDelta ? cached!.Sequence : null,
            Depth = depth,
            Prints = prints,
            Base = based ? cached : null,
            Blobs = written,
            Remembered = remembered,
            Offloaded = referred.Count > 0,
        };
    }

    /// <summary>The outermost envelope of an encoded row.</summary>
    private static JsonObject Envelope(Checkpoint stored, long sequence) =>
        stored.FrozenState as JsonObject
        ?? throw new InvalidOperationException(
            $"checkpoint '{stored.ThreadId}' #{sequence} is stored as {stored.SchemaVersion} without an envelope");

    /// <summary>How many rows back the row <paramref name="stored"/> is from one with no delta: 0 when it has none.</summary>
    public static int DepthOf(Checkpoint stored, long sequence) =>
        IsEncoded(stored.SchemaVersion) && Envelope(stored, sequence)["depth"] is { } depth ? depth.GetValue<int>() : 0;

    /// <summary>
    /// The version a listing reports for a row stored as <paramref name="schemaVersion"/>, given its envelope's
    /// <c>checkpoint_version</c>: listings read no state, so they decode no more than this.
    /// </summary>
    public static string ListedVersion(string schemaVersion, string? checkpointVersion) =>
        IsEncoded(schemaVersion) && checkpointVersion is not null ? checkpointVersion : schemaVersion;

    /// <summary><paramref name="stored"/> with the version it was saved with, for a listing; its states are left encoded.</summary>
    public static Checkpoint Listed(Checkpoint stored) =>
        IsEncoded(stored.SchemaVersion) && stored.FrozenState is JsonObject envelope
            && envelope["checkpoint_version"] is { } version
            ? stored with { SchemaVersion = version.GetValue<string>() }
            : stored;

    /// <summary>
    /// The checkpoints <paramref name="rows"/> (consecutive rows of one thread, oldest first) decode to, from
    /// <paramref name="from"/> on, with their offloaded values still to fetch (<see cref="ResolveAsync"/>): a
    /// <see langword="null"/> holds each one's place. A delta row's base must be the row before it in
    /// <paramref name="rows"/>, so the first row must have none: a store reads a row together with the rows its
    /// <c>depth</c> says it needs. Decoded states keep the base's key order, with changed keys in place, added keys
    /// after and removed keys gone; an offloaded value of a whole level comes after the level's other keys. The
    /// rows' envelopes are taken apart in the process.
    /// </summary>
    public static List<DecodedRow> Decode(IReadOnlyList<(long Sequence, Checkpoint Stored)> rows, int from = 0)
    {
        var decoded = new List<DecodedRow>(Math.Max(0, rows.Count - from));
        if (rows.All(row => !IsEncoded(row.Stored.SchemaVersion)))
        {
            decoded.AddRange(rows.Skip(from).Select(row => new DecodedRow(row.Sequence, row.Stored, [])));
            return decoded;
        }

        // Each level's state as of the row before, owned here and changed in place by the next delta, and the
        // offloaded values it refers to, by key.
        List<JsonNode?>? current = null;
        List<Dictionary<string, BlobSlot>>? pending = null;
        long? previous = null;
        for (var r = 0; r < rows.Count; r++)
        {
            var (sequence, stored) = rows[r];
            var emit = r >= from;
            if (!IsEncoded(stored.SchemaVersion))
            {
                // A row that isn't returned gives its states up; one that is keeps its own.
                var levels = Levels(stored);
                current = [.. levels.Select(level => emit ? level.FrozenState?.DeepClone() : level.FrozenState)];
                pending = [.. levels.Select(_ => new Dictionary<string, BlobSlot>(StringComparer.Ordinal))];
                previous = sequence;
                if (emit)
                {
                    decoded.Add(new DecodedRow(sequence, stored, []));
                }

                continue;
            }

            var outer = Envelope(stored, sequence);
            var deltaOf = outer["delta_of"];
            if (deltaOf is not null && (previous is null || deltaOf.GetValue<long>() != previous))
            {
                throw new InvalidOperationException(
                    $"checkpoint '{stored.ThreadId}' #{sequence} is a delta of #{deltaOf.GetValue<long>()}, which wasn't read with it");
            }

            // A row with no delta_of has no delta level, and needs nothing from the row before.
            var before = deltaOf is null ? null : current;
            var beforePending = deltaOf is null ? null : pending;
            var encodedLevels = Levels(stored);
            var states = new List<JsonNode?>(encodedLevels.Count);
            var slots = new List<Dictionary<string, BlobSlot>>(encodedLevels.Count);
            for (var i = 0; i < encodedLevels.Count; i++)
            {
                var envelope = encodedLevels[i].FrozenState as JsonObject
                    ?? throw new InvalidOperationException($"checkpoint '{stored.ThreadId}' #{sequence} has a level without an envelope");
                Dictionary<string, BlobSlot> levelSlots;
                JsonNode? state;
                if (envelope.ContainsKey("state"))
                {
                    state = envelope["state"];
                    envelope.Remove("state");
                    levelSlots = new Dictionary<string, BlobSlot>(StringComparer.Ordinal);
                }
                else
                {
                    if (before is null || i >= before.Count || before[i] is not JsonObject basis)
                    {
                        throw new InvalidOperationException(
                            $"checkpoint '{stored.ThreadId}' #{sequence} has a delta at level {i}, and the row before has no object there");
                    }

                    levelSlots = beforePending is not null && i < beforePending.Count
                        ? beforePending[i]
                        : new Dictionary<string, BlobSlot>(StringComparer.Ordinal);
                    if (envelope["unset"] is JsonArray unset)
                    {
                        foreach (var key in unset)
                        {
                            basis.Remove(key!.GetValue<string>());
                            levelSlots.Remove(key.GetValue<string>());
                        }
                    }

                    if (envelope["set"] is JsonObject set)
                    {
                        foreach (var key in set.Select(property => property.Key).ToList())
                        {
                            var value = set[key];
                            set.Remove(key);
                            basis[key] = value;
                            levelSlots.Remove(key);
                        }
                    }

                    state = basis;
                }

                if (envelope["blobs"] is JsonObject blobs)
                {
                    var target = state as JsonObject
                        ?? throw new InvalidOperationException(
                            $"checkpoint '{stored.ThreadId}' #{sequence} has blobs at level {i}, whose state isn't an object");
                    foreach (var (key, reference) in blobs)
                    {
                        var hash = reference?["hash"]?.GetValue<string>()
                            ?? throw new InvalidOperationException(
                                $"checkpoint '{stored.ThreadId}' #{sequence} has a blob at '{key}' without a hash");
                        target[key] = null;
                        levelSlots[key] = new BlobSlot(i, key, hash, sequence);
                    }
                }

                states.Add(state);
                slots.Add(levelSlots);
            }

            current = states;
            pending = slots;
            previous = sequence;
            if (emit)
            {
                // The last row may have the states themselves; any other gets copies, which the next row changes.
                var last = r == rows.Count - 1;
                var own = last ? states : [.. states.Select(state => state?.DeepClone())];
                var version = outer["checkpoint_version"]?.GetValue<string>()
                    ?? throw new InvalidOperationException($"checkpoint '{stored.ThreadId}' #{sequence} has no checkpoint_version");
                decoded.Add(new DecodedRow(
                    sequence,
                    WithStates(stored, own) with { SchemaVersion = version },
                    [.. slots.SelectMany(level => level.Values)]));
            }
        }

        return decoded;
    }

    /// <summary>
    /// Puts each offloaded value <paramref name="rows"/> refer to in its place, fetched through
    /// <paramref name="read"/> once per hash, and checked against its hash.
    /// </summary>
    /// <exception cref="InvalidOperationException">A blob is missing, or its content doesn't match its hash.</exception>
    public static async ValueTask<List<DecodedRow>> ResolveAsync(
        List<DecodedRow> rows, BlobReader read, CancellationToken cancellationToken)
    {
        var wanted = new Dictionary<string, BlobSlot>(StringComparer.Ordinal);
        foreach (var slot in rows.SelectMany(row => row.Slots))
        {
            wanted.TryAdd(slot.Hash, slot);
        }

        if (wanted.Count == 0)
        {
            return rows;
        }

        var threadId = rows[0].Checkpoint.ThreadId;
        var found = await read(wanted.Keys, cancellationToken).ConfigureAwait(false);
        var values = new Dictionary<string, JsonNode?>(StringComparer.Ordinal);
        foreach (var (hash, slot) in wanted)
        {
            var refers = $"checkpoint '{threadId}' #{slot.Sequence} refers to blob {hash} at '{slot.Key}'";
            if (!found.TryGetValue(hash, out var content))
            {
                throw new InvalidOperationException($"{refers}, which the blob store doesn't have");
            }

            if (!OffloadedBlobs.Matches(hash, content.Span))
            {
                throw new InvalidOperationException($"{refers}, whose content doesn't match");
            }

            values[hash] = JsonNode.Parse(content.Span);
        }

        foreach (var row in rows)
        {
            if (row.Slots.Count == 0)
            {
                continue;
            }

            var levels = Levels(row.Checkpoint);
            foreach (var slot in row.Slots)
            {
                ((JsonObject)levels[slot.Level].FrozenState!)[slot.Key] = values[slot.Hash]?.DeepClone();
            }
        }

        return rows;
    }

    /// <summary><see cref="Decode"/>, then <see cref="ResolveAsync"/>.</summary>
    public static ValueTask<List<DecodedRow>> DecodeAsync(
        IReadOnlyList<(long Sequence, Checkpoint Stored)> rows, int from, BlobReader read, CancellationToken cancellationToken) =>
        ResolveAsync(Decode(rows, from), read, cancellationToken);

    /// <summary>
    /// <paramref name="rows"/> (a page of a thread's log, oldest first) decoded, reading through
    /// <paramref name="earlier"/> the rows before the page that its first row is a delta of, and through
    /// <paramref name="read"/> the values they offloaded.
    /// </summary>
    public static async ValueTask<List<(long Sequence, Checkpoint Checkpoint)>> DecodePageAsync(
        List<(long Sequence, Checkpoint Stored)> rows,
        Func<long, long, ValueTask<List<(long Sequence, Checkpoint Stored)>>> earlier,
        BlobReader read,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || rows.All(row => !IsEncoded(row.Stored.SchemaVersion)))
        {
            return rows;
        }

        var depth = DepthOf(rows[0].Stored, rows[0].Sequence);
        var all = rows;
        var from = 0;
        if (depth > 0)
        {
            var before = await earlier(rows[0].Sequence - depth, rows[0].Sequence - 1).ConfigureAwait(false);
            all = [.. before, .. rows];
            from = before.Count;
        }

        var decoded = await DecodeAsync(all, from, read, cancellationToken).ConfigureAwait(false);
        return [.. decoded.Select(row => (row.Sequence, row.Checkpoint))];
    }
}

/// <summary>
/// The threads whose latest rows a store remembers, the least recently used forgotten first: what a save is a
/// delta of, and which blobs it needn't write again. Thread-safe.
/// </summary>
internal sealed class DeltaCache(int capacity)
{
    private readonly object _lock = new();
    private readonly Dictionary<string, LinkedListNode<(string Thread, CachedRow Row)>> _rows = new(StringComparer.Ordinal);
    private readonly LinkedList<(string Thread, CachedRow Row)> _order = new();

    public CachedRow? Get(string threadId)
    {
        lock (_lock)
        {
            if (!_rows.TryGetValue(threadId, out var node))
            {
                return null;
            }

            _order.Remove(node);
            _order.AddFirst(node);
            return node.Value.Row;
        }
    }

    /// <summary>
    /// Remembers <paramref name="row"/> as the thread's latest. A save's row replaces whatever is there (it was
    /// the latest when it was written); a load's only one that isn't newer, since a save may have finished
    /// while the load ran.
    /// </summary>
    public void Put(string threadId, CachedRow row, bool fromSave)
    {
        lock (_lock)
        {
            if (_rows.TryGetValue(threadId, out var node))
            {
                if (!fromSave && node.Value.Row.Sequence > row.Sequence)
                {
                    return;
                }

                _order.Remove(node);
                node.Value = (threadId, row);
                _order.AddFirst(node);
                return;
            }

            _rows[threadId] = _order.AddFirst((threadId, row));
            if (_rows.Count > capacity)
            {
                var oldest = _order.Last!;
                _order.RemoveLast();
                _rows.Remove(oldest.Value.Thread);
            }
        }
    }

    public void Forget(string threadId)
    {
        lock (_lock)
        {
            if (_rows.Remove(threadId, out var node))
            {
                _order.Remove(node);
            }
        }
    }

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _rows.Count;
            }
        }
    }
}

/// <summary>
/// A store's half of delta checkpoints and value offload: decides how each save is stored, and remembers what it
/// wrote and read. Correctness never rests on what it remembers: a save that relies on the row before it (a delta
/// of it, or one that leaves out blobs it says the thread has) is appended only if that row is still the thread's
/// latest, by its sequence and its token, checked in the same atomic step as the append, and otherwise is stored
/// whole, with every blob it offloads written again.
/// </summary>
internal sealed class CheckpointWriter
{
    /// <summary>The threads remembered when only offload is on.</summary>
    private const int DefaultCachedThreads = 10_000;

    private readonly int? _fullEvery;
    private readonly int? _threshold;

    private CheckpointWriter(DeltaOptions? deltas, OffloadOptions? offload)
    {
        _fullEvery = deltas?.Checked().FullEvery;
        _threshold = offload?.Checked().Threshold;
        Cache = new DeltaCache(deltas?.CachedThreads ?? DefaultCachedThreads);
    }

    /// <summary>A writer for a store with <paramref name="deltas"/> or <paramref name="offload"/>; none when both are off.</summary>
    public static CheckpointWriter? For(DeltaOptions? deltas, OffloadOptions? offload) =>
        deltas is null && offload is null ? null : new CheckpointWriter(deltas, offload);

    internal DeltaCache Cache { get; }

    public bool WritesDeltas => _fullEvery is not null;

    public bool Offloads => _threshold is not null;

    /// <summary>
    /// How <paramref name="checkpoint"/> is stored: a delta of the thread's latest row when it is running, the
    /// store remembers that row, and its chain is still shorter than <see cref="DeltaOptions.FullEvery"/>; with its
    /// large values offloaded, those the store remembers the thread has not written again (all of them with
    /// <paramref name="writeAllBlobs"/>); whole and with every blob written when <paramref name="alone"/> is false
    /// (another save of its thread is in the same flush, so the row before it isn't known yet).
    /// </summary>
    public EncodedSave Prepare(Checkpoint checkpoint, bool alone = true, bool writeAllBlobs = false)
    {
        var prints = LevelPrints.Of(checkpoint, _threshold, out var large);
        var cached = alone ? Cache.Get(checkpoint.ThreadId) : null;
        var canDelta = _fullEvery is { } every
            && checkpoint.Status == CheckpointStatus.Running
            && cached is not null
            && cached.Depth < every - 1;
        var save = CheckpointEncoding.Encode(checkpoint, prints, large, cached, canDelta, writeAllBlobs);
        if (save.Base is null)
        {
            return save;
        }

        return new EncodedSave
        {
            Checkpoint = save.Checkpoint,
            Stored = save.Stored,
            DeltaOf = save.DeltaOf,
            Depth = save.Depth,
            Prints = save.Prints,
            Base = save.Base,
            Blobs = save.Blobs,
            Remembered = save.Remembered,
            Offloaded = save.Offloaded,
            WholeForm = () => CheckpointEncoding.Encode(checkpoint, prints, large, cached: null, canDelta: false),
        };
    }

    /// <summary>Remembers the row <paramref name="save"/> was written as, at <paramref name="sequence"/>, checked by <paramref name="token"/>.</summary>
    public void Saved(EncodedSave save, long sequence, string token) =>
        Cache.Put(
            save.Checkpoint.ThreadId,
            new CachedRow(sequence, save.Depth, save.Prints ?? LevelPrints.Of(save.Checkpoint), token, save.Remembered),
            fromSave: true);

    /// <summary>
    /// Remembers a thread's latest row, read at <paramref name="sequence"/>, <paramref name="depth"/> rows from a
    /// whole one, with <paramref name="token"/>, referring to the offloaded values of <paramref name="row"/>.
    /// </summary>
    public void Loaded(long sequence, int depth, DecodedRow row, string token) =>
        Cache.Put(
            row.Checkpoint.ThreadId,
            new CachedRow(
                sequence,
                depth,
                LevelPrints.Of(row.Checkpoint, row.Slots),
                token,
                row.Slots.Select(slot => slot.Hash).ToHashSet(StringComparer.Ordinal)),
            fromSave: false);

    public void Forget(string threadId) => Cache.Forget(threadId);
}
