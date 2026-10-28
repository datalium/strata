using System.Collections.Concurrent;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// An embedding generator that answers from a script (<c>design/45-Long-Term-Memory.md</c>, "Tests"): each call's
/// inputs are recorded, then <see cref="Answer"/> makes its embeddings or throws. Its metadata names
/// <paramref name="provider"/> and <paramref name="model"/>.
/// </summary>
internal sealed class ScriptedEmbeddingGenerator(
    string? provider, string? model, Func<IReadOnlyList<string>, GeneratedEmbeddings<Embedding<float>>> answer)
    : IEmbeddingGenerator<string, Embedding<float>>
{
    /// <summary>Every call's inputs, in order.</summary>
    public ConcurrentQueue<IReadOnlyList<string>> Calls { get; } = new();

    /// <summary>Every call's options, in order.</summary>
    public ConcurrentQueue<EmbeddingGenerationOptions?> Options { get; } = new();

    /// <summary>A call waits for its token to fire instead of answering.</summary>
    public bool Hang { get; init; }

    public Func<IReadOnlyList<string>, GeneratedEmbeddings<Embedding<float>>> Answer { get; } = answer;

    /// <summary>One vector per input, <c>[length, 1, 0]</c>, with 4 input tokens per input and the model's name.</summary>
    public static ScriptedEmbeddingGenerator ByLength(string? provider = "scripted", string? model = "embed-1") =>
        new(provider, model, inputs => Embeddings(model, inputs.Select(text => new float[] { text.Length, 1, 0 })));

    /// <summary>Embeddings of <paramref name="vectors"/> by <paramref name="model"/>, with 4 input tokens each and no output count.</summary>
    public static GeneratedEmbeddings<Embedding<float>> Embeddings(string? model, IEnumerable<float[]> vectors)
    {
        var embeddings = new GeneratedEmbeddings<Embedding<float>>(vectors.Select(v => new Embedding<float>(v) { ModelId = model }));
        embeddings.Usage = new UsageDetails { InputTokenCount = 4 * embeddings.Count };
        return embeddings;
    }

    public async Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> inputs = [.. values];
        Calls.Enqueue(inputs);
        Options.Enqueue(options);
        if (Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        await Task.Yield();
        return Answer(inputs);
    }

    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceType == typeof(EmbeddingGeneratorMetadata) ? new EmbeddingGeneratorMetadata(provider, null, model)
        : serviceType.IsInstanceOfType(this) ? this
        : null;

    public void Dispose()
    {
    }
}
