using System.Runtime.InteropServices;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Memory;

namespace Datalium.Strata.Tests.Memory;

public class ZeroCopyAndExecutionContextTests
{
    [Fact]
    public async Task InMemoryStoreReturnsTheSameStringInstanceNotACopy()
    {
        var store = new InMemoryHistoryStore();
        var original = "hello world";
        await store.AppendAsync("s1", new ChatMessage(MessageRole.User, original.AsMemory(), DateTimeOffset.UtcNow));

        ChatMessage? retrieved = null;
        await foreach (var message in store.GetHistoryAsync("s1"))
        {
            retrieved = message;
        }

        Assert.NotNull(retrieved);
        // Zero-copy per design/03-Architecture.md: recovering the backing string from the
        // returned ReadOnlyMemory<char> and comparing by reference (not by value) proves the
        // store handed back the exact same string object, not a freshly allocated copy.
        Assert.True(MemoryMarshal.TryGetString(retrieved.Value.Content, out var text, out _, out _));
        Assert.Same(original, text);
    }

    [Fact]
    public async Task ExecutionContextExposesTheHistoryStoreItWasBuiltWith()
    {
        var history = new InMemoryHistoryStore();
        IExecutionContext ctx = new StrataExecutionContext("thread_1", history);

        await ctx.History.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.User, "hi".AsMemory(), DateTimeOffset.UtcNow));

        var messages = new List<string>();
        await foreach (var message in ctx.History.GetHistoryAsync(ctx.ThreadId))
        {
            messages.Add(message.Content.ToString());
        }

        Assert.Equal("thread_1", ctx.ThreadId);
        Assert.Equal(["hi"], messages);
    }
}
