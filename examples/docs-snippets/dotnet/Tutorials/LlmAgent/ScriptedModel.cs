// The tutorial's ScriptedModel.cs, for https://stratamesh.dev/tutorials/llm-agent/.

// docs:begin tutorials/llm-agent/scripted-model
using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Tutorials.LlmAgent;

// docs:show
// A stand-in for a model: plays one scripted answer per request, in order.
public sealed class ScriptedModel(params ChatResponse[] turns) : IChatClient
{
    private readonly Queue<ChatResponse> _turns = new(turns);

    public static ChatResponse Call(string id, string tool, Dictionary<string, object?> arguments) =>
        new(new ChatMessage(ChatRole.Assistant, [new FunctionCallContent(id, tool, arguments)]));

    public static ChatResponse Say(string text) => new(new ChatMessage(ChatRole.Assistant, text));

    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
        Task.FromResult(_turns.Count > 0 ? _turns.Dequeue() : throw new InvalidOperationException("the script has no more turns"));

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var response = await GetResponseAsync(messages, options, cancellationToken);
        foreach (var update in response.ToChatResponseUpdates())
        {
            yield return update;
        }
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}
// docs:end tutorials/llm-agent/scripted-model
