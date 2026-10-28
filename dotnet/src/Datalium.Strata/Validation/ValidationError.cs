namespace Datalium.Strata.Validation;

/// <summary>
/// Compile-time graph validation (STRATA-SPEC-004). Mirrors
/// <c>rust/strata-core/src/validate.rs</c> — same structural error
/// classes, same <c>STRATA_ERR_*</c> codes.
/// </summary>
public enum ValidationErrorKind
{
    MissingEntryPoint,
    OrphanNode,
    UnguardedCycle,
    DanglingEdge,

    /// <summary>A parallel group with no branches (<c>design/22-Graph-Structure-In-Definitions.md</c>).</summary>
    EmptyGroup,

    /// <summary>Two branches of one parallel group share a name.</summary>
    DuplicateBranch,

    /// <summary>
    /// A parallel group's quorum join is missing its count, or it is below 1 or above the branch count
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>); or a map's is missing or below 1
    /// (<c>design/27-Map-Fan-Out.md</c>).
    /// </summary>
    InvalidQuorum,

    /// <summary>
    /// A wait node inside a map's template, at any depth (<c>design/29-Event-Waits-And-Timers.md</c>): a map
    /// item can't freeze.
    /// </summary>
    WaitInMap,

    /// <summary>
    /// An interrupt boundary inside a map's template, at any depth (<c>design/48-Boundaries-In-Definitions.md</c>):
    /// a map item can't freeze.
    /// </summary>
    BoundaryInMap,
}

public sealed record ValidationError(
    ValidationErrorKind Kind,
    string Code,
    string Node,
    string? RelatedNode,
    string Message);
