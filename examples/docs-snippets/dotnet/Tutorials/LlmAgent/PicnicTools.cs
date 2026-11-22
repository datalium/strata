// The tutorial's PicnicTools.cs, for https://stratamesh.dev/tutorials/llm-agent/.

// docs:begin tutorials/llm-agent/tools
using Datalium.Strata.Tools;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Tutorials.LlmAgent;

// docs:show
public partial class PicnicTools
{
    [StrataTool(Name = "weather.forecast", Description = "The weather forecast for a city on a day of the week.")]
    public static string Forecast(string city, string day)
    {
        Console.WriteLine($"[tool] weather.forecast({city}, {day})");
        return city.Trim().ToLowerInvariant() switch
        {
            "lisbon" => $"{day} in Lisbon: sunny, 24°C, 5% chance of rain",
            "oslo" => $"{day} in Oslo: heavy rain, 9°C, 90% chance of rain",
            _ => $"no forecast for {city}",
        };
    }

    [StrataTool(Name = "parks.search", Description = "Parks in a city where picnics are allowed.")]
    public static string Search(string city)
    {
        Console.WriteLine($"[tool] parks.search({city})");
        return city.Trim().ToLowerInvariant() switch
        {
            "lisbon" => "Jardim da Estrela, Parque Eduardo VII",
            "oslo" => "Frognerparken, St. Hanshaugen",
            _ => $"no parks known in {city}",
        };
    }
}
// docs:end tutorials/llm-agent/tools
