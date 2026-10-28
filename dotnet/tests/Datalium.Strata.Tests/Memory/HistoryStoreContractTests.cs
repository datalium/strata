using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Memory;

namespace Datalium.Strata.Tests.Memory;

/// <summary>
/// Shared <see cref="IHistoryStore"/> contract test suite (Phase 6 exit criterion): the exact
/// same test methods run against both <see cref="InMemoryHistoryStore"/> and
/// <see cref="EfCoreHistoryStore"/> via subclasses overriding <see cref="CreateStore"/> — the
/// xUnit equivalent of the Rust suite's shared <c>run_contract(&amp;dyn HistoryStore)</c>
/// function in <c>rust/strata-memory/tests/history_store_contract.rs</c>.
/// </summary>
public abstract class HistoryStoreContractTests
{
    protected abstract IHistoryStore CreateStore();

    private static ChatMessage Message(MessageRole role, string content, int secondsOffset) =>
        new(role, content.AsMemory(), new DateTimeOffset(2026, 9, 16, 0, 0, 0, TimeSpan.Zero).AddSeconds(secondsOffset));

    private static async Task<List<string>> ContentsAsync(IAsyncEnumerable<ChatMessage> source)
    {
        var results = new List<string>();
        await foreach (var message in source)
        {
            results.Add(message.Content.ToString());
        }

        return results;
    }

    [Fact]
    public async Task UnknownSessionReturnsEmptyHistory()
    {
        var store = CreateStore();
        Assert.Empty(await ContentsAsync(store.GetHistoryAsync("missing")));
    }

    [Fact]
    public async Task AppendThenGetReturnsMessagesInChronologicalOrder()
    {
        var store = CreateStore();
        await store.AppendAsync("s1", Message(MessageRole.User, "hi", 0));
        await store.AppendAsync("s1", Message(MessageRole.Assistant, "hello!", 1));

        Assert.Equal(["hi", "hello!"], await ContentsAsync(store.GetHistoryAsync("s1")));
    }

    [Fact]
    public async Task LimitReturnsOnlyTheMostRecentMessagesStillInOrder()
    {
        var store = CreateStore();
        await store.AppendAsync("s1", Message(MessageRole.User, "first", 0));
        await store.AppendAsync("s1", Message(MessageRole.Assistant, "second", 1));
        await store.AppendAsync("s1", Message(MessageRole.User, "third", 2));

        Assert.Equal(["second", "third"], await ContentsAsync(store.GetHistoryAsync("s1", limit: 2)));
    }

    [Fact]
    public async Task SessionsAreIsolatedFromEachOther()
    {
        var store = CreateStore();
        await store.AppendAsync("s1", Message(MessageRole.User, "s1 message", 0));
        await store.AppendAsync("s2", Message(MessageRole.System, "s2 message", 0));

        Assert.Equal(["s1 message"], await ContentsAsync(store.GetHistoryAsync("s1")));
        Assert.Equal(["s2 message"], await ContentsAsync(store.GetHistoryAsync("s2")));
    }
}

public sealed class InMemoryHistoryStoreContractTests : HistoryStoreContractTests
{
    protected override IHistoryStore CreateStore() => new InMemoryHistoryStore();
}

public sealed class EfCoreHistoryStoreContractTests : HistoryStoreContractTests, IDisposable
{
    private readonly List<EfCoreHistoryStore> _createdStores = [];

    protected override IHistoryStore CreateStore()
    {
        var store = EfCoreHistoryStore.CreateInMemory();
        _createdStores.Add(store);
        return store;
    }

    public void Dispose()
    {
        foreach (var store in _createdStores)
        {
            store.Dispose();
        }
    }
}
