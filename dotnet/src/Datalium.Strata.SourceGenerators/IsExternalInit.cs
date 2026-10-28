// netstandard2.0 (required for Roslyn source generators) predates `init`-accessor and
// record support; the C# compiler only needs this marker type to exist by name, not any
// particular implementation, to allow both.
namespace System.Runtime.CompilerServices;

internal static class IsExternalInit;
