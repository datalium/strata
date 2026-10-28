using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Tools;

/// <summary>
/// Aggregates tool providers and dispatches to them by name, refusing duplicate names at registration
/// (<c>design/20-Tool-Dispatch-And-Registry.md</c>). Built once at startup;
/// <see cref="Subset(IEnumerable{string})"/> gives one agent a curated view. Itself an
/// <see cref="IStrataToolProvider"/>, so a registry or a subset goes wherever a provider does. Mirrors
/// the Rust engine's <c>ToolRegistry</c>.
/// </summary>
/// <remarks>
/// Every dispatch is traced: a <c>strata.tool</c> activity, the <c>strata.tool.*</c> metrics, and,
/// for a call whose context came from a node (<see cref="ToolCallContext.From"/>),
/// <see cref="ToolCallStarted"/> / <see cref="ToolCallCompleted"/> run events. Registration is
/// synchronized and copy-on-write, so it is safe while calls are in flight; dispatch reads a snapshot.
/// Middleware registered with <see cref="Use(IToolMiddleware)"/> runs around every call, inside its
/// activity (<c>design/23-Tool-Middleware.md</c>).
/// </remarks>
public sealed class ToolRegistry : IStrataToolProvider
{
    private readonly Lock _gate = new();
    private Snapshot _snapshot = Snapshot.Empty;

    /// <summary>The registered tools, in registration order.</summary>
    public IReadOnlyList<ToolDescriptor> Tools => Volatile.Read(ref _snapshot).Descriptors;

    /// <summary>The registered middleware, in registration order (outermost first).</summary>
    public IReadOnlyList<IToolMiddleware> Middleware => Volatile.Read(ref _snapshot).Middleware;

    /// <summary>
    /// Adds every tool <paramref name="provider"/> declares, learned from its manifest. Registering a
    /// <see cref="ToolRegistry"/> adds its tools, not itself, so a call is traced once; each keeps that
    /// registry's middleware (as it is now), which runs inside this registry's.
    /// </summary>
    /// <exception cref="DuplicateToolException">A tool of that name is already registered; nothing from
    /// <paramref name="provider"/> is.</exception>
    public ToolRegistry Register(IStrataToolProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        var entries = provider is ToolRegistry registry
            ? Flatten(Volatile.Read(ref registry._snapshot))
            : Parse(provider);
        lock (_gate)
        {
            var current = _snapshot;
            var names = new HashSet<string>(current.Index.Keys, StringComparer.Ordinal);
            foreach (var entry in entries)
            {
                if (!names.Add(entry.Descriptor.Name))
                {
                    throw new DuplicateToolException(entry.Descriptor.Name);
                }
            }

            Volatile.Write(ref _snapshot, current.With(entries: current.Entries.AddRange(entries)));
        }

        return this;
    }

    /// <summary>
    /// Adds <paramref name="middleware"/> to the end of the chain every call runs through, so it runs
    /// inside the middleware registered before it (<c>design/23-Tool-Middleware.md</c>). A subset made
    /// afterwards shares it; one made before doesn't.
    /// </summary>
    public ToolRegistry Use(IToolMiddleware middleware)
    {
        ArgumentNullException.ThrowIfNull(middleware);
        lock (_gate)
        {
            Volatile.Write(ref _snapshot, _snapshot.With(middleware: _snapshot.Middleware.Add(middleware)));
        }

        return this;
    }

    /// <summary>
    /// A new registry holding only the named tools, in this registry's order (not the order given), so
    /// agents sharing tools list them identically. Calling a tool of this registry that the subset left
    /// out throws <see cref="ToolNotPermittedException"/>.
    /// </summary>
    /// <remarks>
    /// A snapshot: tools registered here afterwards don't appear in it. It shares this registry's
    /// middleware as it is now, and middleware added to the subset runs inside it. A repeated name
    /// counts once, and an empty subset is allowed.
    /// </remarks>
    /// <exception cref="ToolNotFoundException">A name this registry never had.</exception>
    /// <exception cref="ToolNotPermittedException">A name this registry had itself excluded (a subset
    /// can only narrow).</exception>
    public ToolRegistry Subset(params IEnumerable<string> toolNames)
    {
        ArgumentNullException.ThrowIfNull(toolNames);
        var snapshot = Volatile.Read(ref _snapshot);
        var chosen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in toolNames)
        {
            if (!snapshot.Index.ContainsKey(name))
            {
                throw snapshot.Missing(name);
            }

            chosen.Add(name);
        }

        var excluded = snapshot.Excluded.Union(
            snapshot.Entries.Where(e => !chosen.Contains(e.Descriptor.Name)).Select(e => e.Descriptor.Name));
        var subset = new ToolRegistry();
        subset._snapshot = new Snapshot(
            snapshot.Entries.Where(e => chosen.Contains(e.Descriptor.Name)).ToImmutableArray(), excluded, snapshot.Middleware);
        return subset;
    }

    /// <summary>The merged JSON-Schema manifest array, in registration order.</summary>
    public string GetToolsManifest() => Volatile.Read(ref _snapshot).Manifest;

    /// <summary>
    /// <see cref="DispatchToolAsync(string, string, ToolCallContext, CancellationToken)"/> with
    /// <see cref="ToolCallContext.None"/>.
    /// </summary>
    public Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>
        DispatchToolAsync(toolName, jsonArguments, ToolCallContext.None);

    /// <summary>
    /// Runs the tool <paramref name="toolName"/> with <paramref name="jsonArguments"/>,
    /// <paramref name="context"/> and <paramref name="cancellationToken"/>.
    /// </summary>
    /// <remarks>
    /// The tool doesn't run if the token has already been cancelled (an
    /// <see cref="OperationCanceledException"/>), and neither does any middleware. Otherwise the call
    /// runs through the middleware, outermost first, and the token reaches the tool, which is not
    /// abandoned if it ignores it; inside a graph, the engine stops waiting for the calling node. A
    /// refused name runs each middleware's <see cref="IToolMiddleware.OnRefusedAsync"/> instead.
    /// </remarks>
    /// <exception cref="ToolNotFoundException">No such tool.</exception>
    /// <exception cref="ToolNotPermittedException">A tool this subset left out.</exception>
    /// <exception cref="ToolCallDeniedException">A middleware refused the call.</exception>
    public async Task<string> DispatchToolAsync(
        string toolName, string jsonArguments, ToolCallContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(toolName);
        ArgumentNullException.ThrowIfNull(context);
        var snapshot = Volatile.Read(ref _snapshot);
        if (context.CallId is null)
        {
            context = context with { CallId = Guid.CreateVersion7().ToString() };
        }

        var found = snapshot.Index.TryGetValue(toolName, out var entry);
        using var scope = new ToolCallScope(toolName, found || snapshot.Excluded.Contains(toolName), context);
        context = context with { Activity = scope.Activity ?? context.Activity };
        try
        {
            if (!found)
            {
                var refusal = snapshot.Missing(toolName);
                var refused = new ToolCall(toolName, jsonArguments, context);
                foreach (var middleware in snapshot.Middleware)
                {
                    await middleware.OnRefusedAsync(refused, refusal, cancellationToken).ConfigureAwait(false);
                }

                throw refusal;
            }

            cancellationToken.ThrowIfCancellationRequested();
            var chain = new Chain(
                toolName,
                entry!.Provider,
                entry.Middleware.IsEmpty ? snapshot.Middleware : snapshot.Middleware.AddRange(entry.Middleware));
            var result = await chain
                .RunAsync(0, new ToolCall(toolName, jsonArguments, context, entry.Descriptor), cancellationToken)
                .ConfigureAwait(false);
            if (!chain.ToolRan)
            {
                scope.ShortCircuited();
            }

            scope.Succeeded();
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            scope.Cancelled();
            throw;
        }
        catch (Exception ex)
        {
            scope.Failed(ex);
            throw;
        }
    }

    /// <summary>
    /// <paramref name="snapshot"/>'s tools, each with that registry's middleware ahead of whatever it
    /// already carried, for registering that registry into another.
    /// </summary>
    private static ImmutableArray<Entry> Flatten(Snapshot snapshot) =>
        snapshot.Middleware.IsEmpty
            ? snapshot.Entries
            : snapshot.Entries.Select(e => e with { Middleware = snapshot.Middleware.AddRange(e.Middleware) }).ToImmutableArray();

    /// <summary>One call's middleware chain, ending at the tool's provider; remembers whether the tool ran.</summary>
    private sealed class Chain(string toolName, IStrataToolProvider provider, ImmutableArray<IToolMiddleware> middleware)
    {
        private int _toolRan;

        public bool ToolRan => Volatile.Read(ref _toolRan) == 1;

        public Task<string> RunAsync(int index, ToolCall call, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(call);
            if (index == middleware.Length)
            {
                Volatile.Write(ref _toolRan, 1);
                // The resolved name, not the call's: a middleware can't route to another tool.
                return provider.DispatchToolAsync(toolName, call.Arguments, call.Context, cancellationToken);
            }

            return middleware[index].InvokeAsync(call, (next, token) => RunAsync(index + 1, next, token), cancellationToken);
        }
    }

    private static ImmutableArray<Entry> Parse(IStrataToolProvider provider)
    {
        using var manifest = JsonDocument.Parse(provider.GetToolsManifest());
        if (manifest.RootElement.ValueKind != JsonValueKind.Array)
        {
            throw new ArgumentException("A tool provider's manifest must be a JSON array.", nameof(provider));
        }

        var entries = ImmutableArray.CreateBuilder<Entry>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var element in manifest.RootElement.EnumerateArray())
        {
            var schema = element.Clone();
            var name = schema.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String
                ? n.GetString()!
                : throw new ArgumentException("Every manifest entry needs a string 'name'.", nameof(provider));
            if (!seen.Add(name))
            {
                throw new DuplicateToolException(name);
            }

            var description = schema.TryGetProperty("description", out var d) && d.ValueKind == JsonValueKind.String
                ? d.GetString()!
                : string.Empty;
            var parameters = schema.TryGetProperty("parameters", out var p) ? p : EmptyParameters;
            entries.Add(new Entry(new ToolDescriptor(name, description, parameters), schema, provider, []));
        }

        return entries.ToImmutable();
    }

    private static readonly JsonElement EmptyParameters =
        JsonDocument.Parse("""{"type":"object","properties":{},"required":[]}""").RootElement.Clone();

    /// <param name="Middleware">The middleware of a registry this tool was registered through, which
    /// runs inside the dispatching registry's.</param>
    private sealed record Entry(
        ToolDescriptor Descriptor, JsonElement Schema, IStrataToolProvider Provider, ImmutableArray<IToolMiddleware> Middleware);

    /// <summary>
    /// One immutable state of a registry: its tools, an index, the names a subset left out, the
    /// middleware, and the manifest.
    /// </summary>
    private sealed class Snapshot
    {
        public static readonly Snapshot Empty = new([], [], []);

        public Snapshot(ImmutableArray<Entry> entries, ImmutableHashSet<string> excluded, ImmutableArray<IToolMiddleware> middleware)
        {
            Entries = entries;
            Excluded = excluded;
            Middleware = middleware;
            Index = entries.ToImmutableDictionary(e => e.Descriptor.Name, StringComparer.Ordinal);
            Descriptors = entries.Select(e => e.Descriptor).ToImmutableArray();
            Manifest = Render(entries);
        }

        public ImmutableArray<Entry> Entries { get; }

        public ImmutableHashSet<string> Excluded { get; }

        public ImmutableDictionary<string, Entry> Index { get; }

        public ImmutableArray<ToolDescriptor> Descriptors { get; }

        public string Manifest { get; }

        public ImmutableArray<IToolMiddleware> Middleware { get; }

        public Snapshot With(ImmutableArray<Entry>? entries = null, ImmutableArray<IToolMiddleware>? middleware = null) =>
            new(entries ?? Entries, Excluded, middleware ?? Middleware);

        /// <summary>Why <paramref name="name"/> isn't here: left out by a subset, or never registered.</summary>
        public ToolNotFoundException Missing(string name) =>
            Excluded.Contains(name) ? new ToolNotPermittedException(name) : new ToolNotFoundException(name);

        private static string Render(ImmutableArray<Entry> entries)
        {
            using var buffer = new MemoryStream();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartArray();
                foreach (var entry in entries)
                {
                    entry.Schema.WriteTo(writer);
                }

                writer.WriteEndArray();
            }

            return Encoding.UTF8.GetString(buffer.ToArray());
        }
    }
}
