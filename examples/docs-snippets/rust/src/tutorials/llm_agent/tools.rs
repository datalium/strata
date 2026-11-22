//! The tutorial's `src/tools.rs`, for <https://stratamesh.dev/tutorials/llm-agent/>.

// docs:begin tutorials/llm-agent/tools
use datalium_strata::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, StrataSchema)]
pub struct ForecastArgs {
    city: String,
    day: String,
}

#[strata_tool(
    name = "weather.forecast",
    description = "The weather forecast for a city on a day of the week."
)]
pub async fn weather_forecast(args: ForecastArgs) -> Result<String, StrataError> {
    println!("[tool] weather.forecast({}, {})", args.city, args.day);
    Ok(match args.city.trim().to_lowercase().as_str() {
        "lisbon" => format!("{} in Lisbon: sunny, 24°C, 5% chance of rain", args.day),
        "oslo" => format!("{} in Oslo: heavy rain, 9°C, 90% chance of rain", args.day),
        _ => format!("no forecast for {}", args.city),
    })
}

#[derive(Deserialize, StrataSchema)]
pub struct ParksArgs {
    city: String,
}

#[strata_tool(
    name = "parks.search",
    description = "Parks in a city where picnics are allowed."
)]
pub async fn parks_search(args: ParksArgs) -> Result<String, StrataError> {
    println!("[tool] parks.search({})", args.city);
    Ok(match args.city.trim().to_lowercase().as_str() {
        "lisbon" => "Jardim da Estrela, Parque Eduardo VII".to_string(),
        "oslo" => "Frognerparken, St. Hanshaugen".to_string(),
        _ => format!("no parks known in {}", args.city),
    })
}
// docs:end tutorials/llm-agent/tools
