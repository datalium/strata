using BenchmarkDotNet.Running;
using Datalium.Strata.Benchmarks;

// design/37-Benchmarks.md (SG-27). Two things run from here:
//
//   -- profile [--quick] [--only <scenario>[,…]] [--out <file>]   the profile (Profile.cs)
//   -- --filter '*' [BenchmarkDotNet's own arguments]               the microbenchmarks (EngineBenchmarks.cs)
if (args is ["profile", .. var profileArguments])
{
    return await Profile.RunAsync(profileArguments);
}

if (args.Length == 0)
{
    Console.Error.WriteLine("usage: profile [--quick] [--only <scenario>[,...]] [--out <file>]");
    Console.Error.WriteLine("       --filter '*' [BenchmarkDotNet arguments]");
    return 2;
}

var summaries = BenchmarkSwitcher.FromAssembly(typeof(Profile).Assembly).Run(args);

// A benchmark that threw is reported by BenchmarkDotNet and would otherwise still exit 0.
return summaries.Any(summary => summary.HasCriticalValidationErrors || summary.Reports.Any(report => !report.Success)) ? 1 : 0;
