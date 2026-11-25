namespace Datalium.Strata.DocsSnippets.Guides.Production;

/// <summary>
/// The tests that read what an OpenTelemetry provider exported run alone. A provider listens to Strata's
/// source and meter in the whole process, so another test's run would add spans to the in-memory exporter's
/// list while the test reads it ("Collection was modified").
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TelemetryExporters
{
    public const string Name = "guides/production telemetry exporters";
}
