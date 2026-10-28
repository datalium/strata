using Datalium.Strata.Example.DotnetAgent;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSupportTickets();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
