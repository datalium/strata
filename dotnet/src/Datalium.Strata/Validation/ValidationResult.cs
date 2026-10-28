namespace Datalium.Strata.Validation;

public sealed class ValidationResult
{
    public List<ValidationError> Errors { get; } = [];

    public bool IsValid => Errors.Count == 0;
}
