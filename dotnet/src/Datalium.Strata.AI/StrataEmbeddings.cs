using System.Globalization;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>
/// An embedding call from a node (<c>design/45-Long-Term-Memory.md</c>, "AI packages: embeddings and the helper"):
/// any <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/>, observed as a model call whose operation is
/// <c>embeddings</c>, and failed the way a node fails. <see cref="StrataMemory"/> embeds through it. Mirrors
/// <c>strata-ai</c>'s <c>embed_with</c>.
/// </summary>
public static class StrataEmbeddings
{
    /// <summary>The GenAI operation an embedding call is recorded as.</summary>
    public const string Operation = "embeddings";

    /// <summary>
    /// Asks <paramref name="generator"/> for one embedding per text of <paramref name="texts"/>, in one call.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>No texts: an empty answer at once, with no call and nothing recorded.</item>
    /// <item>It is recorded through a <see cref="ModelCallScope"/> with operation <c>embeddings</c>:
    /// <see cref="ModelCallStarted"/> and <see cref="ModelCallCompleted"/> (<c>Operation</c> <c>embeddings</c>), an
    /// <c>embeddings &lt;model&gt;</c> span, and the GenAI metrics with the input tokens only, whatever the generator
    /// reported for output. The model is <paramref name="options"/>' <c>ModelId</c>, else the generator's default,
    /// else <c>unknown</c> (a blank one is none); the provider is the generator's. It never streams: a run's stream gets nothing.</item>
    /// <item>An answer with another number of embeddings than texts is a fatal failure,
    /// <c>model call failed (invalid_response): expected 2 vectors, the embedder returned 1</c>.</item>
    /// <item>A fault throws a <see cref="NodeException"/> as <see cref="StrataChat"/>'s does: transient with the
    /// provider's retry hint, or fatal (<see cref="ModelFaults"/>). The node's own cancellation is rethrown as it is,
    /// and a generator's own <see cref="NodeException"/> passes through.</item>
    /// </list>
    /// </remarks>
    public static async Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEmbeddingGenerator<string, Embedding<float>> generator,
        IEnumerable<string> texts,
        EmbeddingGenerationOptions? options = null,
        NodeContext? context = null,
        StrataChatOptions? strata = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(generator);
        ArgumentNullException.ThrowIfNull(texts);
        var inputs = texts as IReadOnlyList<string> ?? [.. texts];
        if (inputs.Count == 0)
        {
            return new GeneratedEmbeddings<Embedding<float>>();
        }

        var metadata = generator.GetService<EmbeddingGeneratorMetadata>();
        var model = StrataChat.ModelOf(options?.ModelId, metadata?.DefaultModelId);
        using var scope = ModelCallScope.Start(context, new ModelCallInfo(model, metadata?.ProviderName)
        {
            Operation = Operation,
            Agent = strata?.Agent,
        });

        GeneratedEmbeddings<Embedding<float>> embeddings;
        try
        {
            embeddings = await generator.GenerateAsync(inputs, options, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (StrataChat.Settle(scope, exception, strata?.FaultClassifier, started: false, cancellationToken) is { } failure)
            {
                throw failure;
            }

            throw;
        }

        if (embeddings.Count != inputs.Count)
        {
            // A vector short (or over) can't be matched to its text: retrying wouldn't make the answer right.
            var message = string.Create(
                CultureInfo.InvariantCulture, $"expected {inputs.Count} vectors, the embedder returned {embeddings.Count}");
            const string errorType = "invalid_response";
            scope.Failed(errorType, message);
            throw ModelFaults.ToNodeException(new ModelFault(false, null, errorType), new InvalidOperationException(message));
        }

        scope.Succeeded(
            new ModelUsage(embeddings.Usage?.InputTokenCount, null), null, embeddings.Count > 0 ? embeddings[0].ModelId : null);
        return embeddings;
    }
}
