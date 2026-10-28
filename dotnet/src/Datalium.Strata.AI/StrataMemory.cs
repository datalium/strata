using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Memory;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>What <see cref="StrataMemory"/> needs besides its store and its embedding generator.</summary>
public sealed record StrataMemoryOptions
{
    /// <summary>
    /// The label the memory's vectors carry (<see cref="MemoryRecord.Model"/>), used as it is. <see langword="null"/>
    /// (the default): <c>&lt;provider&gt;/&lt;model&gt;</c> from the generator's metadata. Set it when the generator
    /// names no model, or to keep a label across providers that serve the same model.
    /// </summary>
    public string? Model { get; init; }

    /// <summary>
    /// The run metadata key an entry's or a recall's tenant is read from when it names none: <c>tenant</c> by
    /// default, as the run scheduler's (<c>design/40-Run-Scheduler.md</c>).
    /// </summary>
    public string TenantKey { get; init; } = "tenant";

    /// <summary>Reads a provider's own exceptions before the default does (<see cref="ModelFaults.Classify"/>).</summary>
    public IModelFaultClassifier? FaultClassifier { get; init; }
}

/// <summary>
/// What <see cref="StrataMemory.RememberAsync"/> takes: a memory's key, its text (which is embedded and kept as
/// <see cref="MemoryRecord.Text"/>), and what the record carries besides.
/// </summary>
/// <param name="Namespace">The kind of memory (<c>incidents</c>, <c>runbooks</c>).</param>
/// <param name="Id">Unique within the namespace and tenant.</param>
/// <param name="Text">What is embedded.</param>
public sealed record MemoryEntry(string Namespace, string Id, string Text)
{
    /// <summary>
    /// Whose memory it is, <c>""</c> for none. <see langword="null"/> (the default): the run's metadata at
    /// <see cref="StrataMemoryOptions.TenantKey"/>, or <c>""</c> outside a run or without it.
    /// </summary>
    public string? Tenant { get; init; }

    /// <summary>The application's payload, never searched.</summary>
    public JsonObject? Content { get; init; }

    /// <summary>String pairs a recall can filter on by equality.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = MemoryRecords.NoMetadata;
}

/// <summary>What <see cref="StrataMemory.RecallAsync"/> searches besides the namespace and the text.</summary>
public sealed record RecallOptions
{
    /// <summary>
    /// The tenant searched, <c>""</c> for records without one. <see langword="null"/> (the default): the run's
    /// metadata at <see cref="StrataMemoryOptions.TenantKey"/>, or <c>""</c> outside a run or without it.
    /// </summary>
    public string? Tenant { get; init; }

    /// <summary>How many matches at most: 1 to <see cref="MemoryRecords.MaxLimit"/>.</summary>
    public int Limit { get; init; } = 10;

    /// <summary>Drops matches less similar than this, from −1 to 1. None by default.</summary>
    public double? MinSimilarity { get; init; }

    /// <summary>Ids never returned (the record the recall is made for, say).</summary>
    public IReadOnlyCollection<string> Exclude { get; init; } = [];

    /// <summary>Only records whose metadata has every one of these pairs.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = MemoryRecords.NoMetadata;
}

/// <summary>
/// Long-term memory by text (<c>design/45-Long-Term-Memory.md</c>, "The helper"): an <see cref="IMemoryStore"/> and an
/// <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> together. <see cref="RememberAsync"/> embeds an entry's text
/// and stores it with its vector; <see cref="RecallAsync"/> embeds a text and searches. Every embedding goes through
/// <see cref="StrataEmbeddings.GenerateAsync"/>, so it is observed in the run as a model call (operation
/// <c>embeddings</c>) and fails as one: a <see cref="NodeException"/>, transient with the provider's hint or fatal,
/// which a node's <c>RecoveryPolicy</c> retries. Mirrors <c>strata-ai</c>'s <c>Memory</c>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Vectors are labelled <see cref="Model"/>: <see cref="StrataMemoryOptions.Model"/>, else
/// <c>&lt;provider&gt;/&lt;model&gt;</c> from the generator's metadata (<c>unknown</c> for a provider it doesn't
/// name). A search reads only its own label's records, so a memory never compares two models' vectors. The embedding
/// request names no model: the generator's default is the one labelled.</item>
/// <item>The tenant is the entry's or the recall's own when set (<c>""</c> included), else the run's metadata at
/// <see cref="StrataMemoryOptions.TenantKey"/> when a node context is given and has it, else <c>""</c>.</item>
/// <item>Bad input is refused before the embedding call, with the store's messages
/// (<see cref="ArgumentException"/>), so a refused call costs nothing. An embedding that fails stores nothing.</item>
/// <item>It never retries and never swallows: a store's failure is the store's (<see cref="MemoryStoreException"/>),
/// for the caller to handle.</item>
/// </list>
/// </remarks>
public sealed class StrataMemory
{
    private readonly IEmbeddingGenerator<string, Embedding<float>> _generator;
    private readonly string _tenantKey;
    private readonly StrataChatOptions _call;

    /// <exception cref="ArgumentException"><see cref="StrataMemoryOptions.Model"/> is empty or blank.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="options"/> has no <see cref="StrataMemoryOptions.Model"/> and the generator names no model (a
    /// blank one is none).
    /// </exception>
    public StrataMemory(
        IMemoryStore store, IEmbeddingGenerator<string, Embedding<float>> generator, StrataMemoryOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(generator);
        options ??= new StrataMemoryOptions();
        ArgumentNullException.ThrowIfNull(options.TenantKey);
        Store = store;
        _generator = generator;
        _tenantKey = options.TenantKey;
        _call = new StrataChatOptions { FaultClassifier = options.FaultClassifier };
        if (options.Model is not null && string.IsNullOrWhiteSpace(options.Model))
        {
            // The store's own refusal, made before any call pays for an embedding.
            throw new ArgumentException("the model is empty");
        }

        Model = options.Model ?? Label(generator.GetService<EmbeddingGeneratorMetadata>());
    }

    /// <summary>The store the memory reads and writes.</summary>
    public IMemoryStore Store { get; }

    /// <summary>The label of the memory's vectors.</summary>
    public string Model { get; }

    /// <summary>
    /// Embeds <paramref name="entry"/>'s text and stores it: a replacing upsert of the record at its namespace,
    /// tenant and id, with the vector, <see cref="Model"/>, the text, the content and the metadata.
    /// </summary>
    /// <param name="context">The calling node's context: the call's run, and the tenant when the entry names none.</param>
    /// <exception cref="ArgumentException">The key or the metadata is one no record can have; nothing was embedded.</exception>
    /// <exception cref="NodeException">The embedding call failed; nothing was stored.</exception>
    public async Task RememberAsync(MemoryEntry entry, NodeContext? context = null, CancellationToken cancellationToken = default)
    {
        var tenant = Checked(entry, context);
        var embeddings = await EmbedAsync([entry.Text], context, cancellationToken).ConfigureAwait(false);
        await Store.UpsertAsync(Record(entry, tenant, embeddings[0]), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="RememberAsync"/> for every one of <paramref name="entries"/>: every entry is checked first, every
    /// text embedded in one call, in order, and every record written in one <see cref="IMemoryStore.UpsertManyAsync"/>.
    /// None: nothing is called.
    /// </summary>
    public async Task RememberManyAsync(
        IReadOnlyList<MemoryEntry> entries, NodeContext? context = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return;
        }

        var tenants = entries.Select(entry => Checked(entry, context)).ToList();
        var embeddings = await EmbedAsync([.. entries.Select(entry => entry.Text)], context, cancellationToken)
            .ConfigureAwait(false);
        var records = entries.Select((entry, i) => Record(entry, tenants[i], embeddings[i])).ToList();
        await Store.UpsertManyAsync(records, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The memories of <paramref name="namespace"/> most like <paramref name="text"/>: the text is embedded and the
    /// store searched for <see cref="Model"/>'s records of the tenant, most similar first, ties by id.
    /// </summary>
    /// <exception cref="ArgumentException">The search is one no store can run; nothing was embedded.</exception>
    /// <exception cref="NodeException">The embedding call failed.</exception>
    public async Task<IReadOnlyList<MemoryMatch>> RecallAsync(
        string @namespace,
        string text,
        RecallOptions? options = null,
        NodeContext? context = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(text);
        options ??= new RecallOptions();
        var search = new MemorySearch(@namespace, ReadOnlyMemory<float>.Empty, Model)
        {
            Tenant = Tenant(options.Tenant, context),
            Limit = options.Limit,
            MinSimilarity = options.MinSimilarity,
            Exclude = options.Exclude,
            Metadata = options.Metadata,
        };
        MemoryRecords.CheckFilters(search);
        var embeddings = await EmbedAsync([text], context, cancellationToken).ConfigureAwait(false);
        return await Store.SearchAsync(search with { Vector = embeddings[0].Vector }, cancellationToken).ConfigureAwait(false);
    }

    private static string Label(EmbeddingGeneratorMetadata? metadata) =>
        !string.IsNullOrWhiteSpace(metadata?.DefaultModelId)
            ? $"{(string.IsNullOrEmpty(metadata.ProviderName) ? "unknown" : metadata.ProviderName)}/{metadata.DefaultModelId}"
            : throw new InvalidOperationException(
                "the embedding generator names no model: label the memory's vectors with StrataMemoryOptions.Model");

    /// <summary>Refuses an entry no record can be made of, before anything is embedded; returns its tenant.</summary>
    private string Checked(MemoryEntry entry, NodeContext? context)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(entry.Text);
        var tenant = Tenant(entry.Tenant, context);
        MemoryRecords.Check(new MemoryKey(entry.Namespace, tenant, entry.Id));
        MemoryRecords.CheckMetadata(entry.Metadata);
        return tenant;
    }

    private string Tenant(string? own, NodeContext? context) =>
        own ?? (context is not null && context.Metadata.TryGetValue(_tenantKey, out var tenant) ? tenant : "");

    private MemoryRecord Record(MemoryEntry entry, string tenant, Embedding<float> embedding) =>
        new(entry.Namespace, tenant, entry.Id, embedding.Vector, Model)
        {
            Text = entry.Text,
            Content = entry.Content,
            Metadata = entry.Metadata,
        };

    private Task<GeneratedEmbeddings<Embedding<float>>> EmbedAsync(
        IReadOnlyList<string> texts, NodeContext? context, CancellationToken cancellationToken) =>
        StrataEmbeddings.GenerateAsync(_generator, texts, null, context, _call, cancellationToken);
}
