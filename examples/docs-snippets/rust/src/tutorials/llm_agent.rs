//! Samples for <https://stratamesh.dev/tutorials/llm-agent/>. The tutorial builds one program: its
//! `src/main.rs` is this file's `program` region, and its `src/tools.rs` and `src/scripted_model.rs` are
//! `llm_agent/tools.rs` and `llm_agent/scripted_model.rs`. The steps that show code in another order than
//! `main.rs` are in the tests below it, with their own copies of the types.
//!
//! The program prints what it does and picks its model from its command line, so the tests that check its
//! output run it in a child process (`run_alone`).

// docs:begin tutorials/llm-agent/program
mod scripted_model;
mod tools;

use std::io::Write;
use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::{RecoveryPolicy, StateGraph};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, ChatClient, Message, OpenAiChatClient, OpenAiOptions,
    StateGraphAiExt,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use scripted_model::ScriptedModel;
use serde::{Deserialize, Serialize};
use tools::{ParksSearchTool, WeatherForecastTool};

#[derive(Debug, Clone, Serialize, Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
struct PicnicPlan {
    city: String,
    day: String,
    go_ahead: bool,
    park: Option<String>,
    reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PicnicRequest {
    city: String,
    day: String,
    plan: Option<PicnicPlan>,
    turns: u32,
    tools_used: Vec<String>,
    message: Option<String>,
}

fn validate(plan: &PicnicPlan) -> Vec<String> {
    let has_park = plan
        .park
        .as_deref()
        .is_some_and(|park| !park.trim().is_empty());
    if plan.go_ahead && !has_park {
        vec!["a picnic that goes ahead needs a park".to_string()]
    } else {
        Vec::new()
    }
}

// docs:begin tutorials/llm-agent/script
fn script() -> ScriptedModel {
    ScriptedModel::new(vec![
        ScriptedModel::call(
            "call_1",
            "weather_forecast",
            r#"{"city":"Lisbon","day":"Saturday"}"#,
        ),
        ScriptedModel::call("call_2", "parks_search", r#"{"city":"Lisbon"}"#),
        ScriptedModel::say(
            r#"{"city":"Lisbon","day":"Saturday","goAhead":true,"park":"Jardim da Estrela","reason":"Sunny and 24°C, with a 5% chance of rain."}"#,
        ),
    ])
}
// docs:end tutorials/llm-agent/script

#[tokio::main]
async fn main() {
    // 1. The chat client: a local model, or a scripted stand-in with --scripted.
    // docs:begin tutorials/llm-agent/choose-client
    let chat: Arc<dyn ChatClient> = if std::env::args().any(|arg| arg == "--scripted") {
        Arc::new(script())
    } else {
        Arc::new(OpenAiChatClient::new(
            OpenAiOptions::ollama("http://localhost:11434").with_model("qwen2.5:3b"),
        ))
    };
    // docs:end tutorials/llm-agent/choose-client

    // 2. The tools the agent may call.
    // docs:begin tutorials/llm-agent/register
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(WeatherForecastTool))
        .register(Box::new(ParksSearchTool));
    // docs:end tutorials/llm-agent/register

    // 3. The agent, with a typed answer.
    // docs:begin tutorials/llm-agent/agent
    let agent = Arc::new(Agent::<PicnicPlan>::new(
        chat,
        Arc::new(tools),
        AgentOptions::new("picnic-planner")
            .with_temperature(0.0)
            .with_max_turns(4)
            .with_validate(validate)
            // Ollama holds the model to the response format on every turn that asks for one, so
            // the model can't call a tool there. Ask for the JSON schema only on a repair turn.
            .with_configure_turn(|turn, request| {
                if !turn.is_repair {
                    request.response_format = None;
                }
            }),
    ));
    // docs:end tutorials/llm-agent/agent

    // 4. The graph: the agent plans, then a plain node writes the message.
    let graph = StateGraph::<PicnicRequest>::new()
        .add_agent_node(
            "Plan",
            agent,
            |request: &PicnicRequest, _| {
                vec![
                    Message::system(
                        "You plan picnics. Always call weather.forecast and parks.search before you answer. \
                         Go ahead only if the forecast is dry. Then answer with only a JSON object with the fields \
                         city, day, goAhead (true or false), park (one park from parks.search, or null) and reason.",
                    ),
                    Message::user(format!(
                        "Can I have a picnic in {} on {}?",
                        request.city, request.day
                    )),
                ]
            },
            |request: PicnicRequest, result: AgentResult<PicnicPlan>| PicnicRequest {
                tools_used: result.tool_calls.iter().map(|call| call.name.clone()).collect(),
                turns: result.turns,
                plan: Some(result.output),
                ..request
            },
        )
        .add_node("Announce", |request: PicnicRequest| {
            let plan = request.plan.clone().expect("Plan ran first");
            let message = if plan.go_ahead {
                format!(
                    "Picnic on: {}, {}. {}",
                    plan.park.unwrap_or_default(),
                    request.day,
                    plan.reason
                )
            } else {
                format!("No picnic {}. {}", request.day, plan.reason)
            };
            PicnicRequest {
                message: Some(message),
                ..request
            }
        })
        .add_edge("Plan", "Announce")
        .set_recovery_policy("Plan", RecoveryPolicy::retry(1))
        .compile()
        .expect("the graph is valid");

    // 5. Run it, streaming the model's text as it arrives.
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let request = PicnicRequest {
        city: "Lisbon".to_string(),
        day: "Saturday".to_string(),
        ..Default::default()
    };
    // docs:begin tutorials/llm-agent/stream
    let (handle, mut chunks) = Arc::new(graph).run_streaming("picnic-1", request, store);

    while let Some(chunk) = chunks.recv().await {
        match chunk.chunk_type {
            StreamChunkType::Token => {
                print!("{}", chunk.content);
                std::io::stdout().flush().ok();
            }
            StreamChunkType::Retry => println!("\n[retry] {}", chunk.node_name),
            _ => {}
        }
    }

    match handle.await.expect("the run's task doesn't panic") {
        GraphExecutionResult::Completed(done) => {
            println!();
            println!("tools used: {}", done.tools_used.join(", "));
            println!("model turns: {}", done.turns);
            println!("{}", done.message.unwrap_or_default());
        }
        GraphExecutionResult::Failed(error) => println!("\nThe run failed: {error:?}"),
        _ => println!("\nThe run did not complete."),
    }
    // docs:end tutorials/llm-agent/stream
}
// docs:end tutorials/llm-agent/program

use datalium_strata_ai::{AnthropicChatClient, AnthropicOptions};

/// What the scripted run prints, on every run (the page shows it after step 8).
const SCRIPTED_OUTPUT: &str = r#"[tool] weather.forecast(Lisbon, Saturday)
[tool] parks.search(Lisbon)
{"city":"Lisbon","day":"Saturday","goAhead":true,"park":"Jardim da Estrela","reason":"Sunny and 24°C, with a 5% chance of rain."}
tools used: weather.forecast, parks.search
model turns: 3
Picnic on: Jardim da Estrela, Saturday. Sunny and 24°C, with a 5% chance of rain.
"#;

/// Runs one test of this binary in a child process, with `args` on its command line after `--` and `envs` set,
/// and returns what it printed. The child's own arguments reach `std::env::args()`, as `cargo run -- --scripted`
/// would pass them.
fn run_alone(test: &str, args: &[&str], envs: &[(&str, &str)]) -> String {
    let output =
        std::process::Command::new(std::env::current_exe().expect("the test binary's path"))
            .args(["--exact", test, "--nocapture", "--test-threads=1", "--"])
            .args(args)
            .envs(envs.iter().copied())
            .output()
            .expect("the test binary runs");
    let stdout = String::from_utf8(output.stdout).expect("the output is UTF-8");
    assert!(
        output.status.success(),
        "{test} failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("1 passed"), "{test} didn't run:\n{stdout}");
    stdout
}

/// The program, as `cargo run -- --scripted` runs it. Without `--scripted` it would call Ollama, so on its own
/// it does nothing: `program_runs_scripted` runs it with the switch.
#[test]
fn scripted_main() {
    if std::env::args().any(|arg| arg == "--scripted") {
        main();
    }
}

#[test]
fn program_runs_scripted() {
    let stdout = run_alone("tutorials::llm_agent::scripted_main", &["--scripted"], &[]);
    assert!(
        stdout.contains(SCRIPTED_OUTPUT),
        "the program printed:\n{stdout}"
    );
}

#[test]
fn create_client() {
    // docs:begin tutorials/llm-agent/create-client
    let chat: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(
        OpenAiOptions::ollama("http://localhost:11434").with_model("qwen2.5:3b"),
    ));
    // docs:end tutorials/llm-agent/create-client

    assert_eq!(chat.provider(), "ollama");
    assert_eq!(chat.default_model(), Some("qwen2.5:3b"));
    // OpenAiOptions::ollama adds /v1 itself.
    assert_eq!(
        OpenAiOptions::ollama("http://localhost:11434").base_url,
        "http://localhost:11434/v1"
    );
}

#[test]
fn answer_type() {
    // docs:begin tutorials/llm-agent/answer-type
    #[derive(Debug, Clone, Serialize, Deserialize, StrataSchema)]
    #[serde(rename_all = "camelCase")]
    struct PicnicPlan {
        city: String,
        day: String,
        go_ahead: bool,
        park: Option<String>,
        reason: String,
    }

    fn validate(plan: &PicnicPlan) -> Vec<String> {
        let has_park = plan
            .park
            .as_deref()
            .is_some_and(|park| !park.trim().is_empty());
        if plan.go_ahead && !has_park {
            vec!["a picnic that goes ahead needs a park".to_string()]
        } else {
            Vec::new()
        }
    }
    // docs:end tutorials/llm-agent/answer-type

    // The fields have the same JSON names as in C#.
    let schema = PicnicPlan::json_schema().to_string();
    assert!(schema.contains("\"goAhead\""), "{schema}");
    let plan: PicnicPlan = serde_json::from_str(
        r#"{"city":"Lisbon","day":"Saturday","goAhead":true,"park":null,"reason":"Sunny."}"#,
    )
    .expect("the answer parses");
    assert_eq!(validate(&plan), ["a picnic that goes ahead needs a park"]);

    let with_park = PicnicPlan {
        park: Some("Jardim da Estrela".to_string()),
        ..plan.clone()
    };
    assert!(validate(&with_park).is_empty());
    let no_picnic = PicnicPlan {
        go_ahead: false,
        ..plan
    };
    assert!(validate(&no_picnic).is_empty());
}

/// Steps 5 and 6: the graph, run without streaming.
#[tokio::test]
async fn graph_and_run() {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(WeatherForecastTool))
        .register(Box::new(ParksSearchTool));
    let agent = Arc::new(Agent::<PicnicPlan>::new(
        Arc::new(script()),
        Arc::new(tools),
        AgentOptions::new("picnic-planner")
            .with_temperature(0.0)
            .with_max_turns(4)
            .with_validate(validate)
            .with_configure_turn(|turn, request| {
                if !turn.is_repair {
                    request.response_format = None;
                }
            }),
    ));

    // docs:begin tutorials/llm-agent/graph
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct PicnicRequest {
        city: String,
        day: String,
        plan: Option<PicnicPlan>,
        turns: u32,
        tools_used: Vec<String>,
        message: Option<String>,
    }

    let graph = StateGraph::<PicnicRequest>::new()
        .add_agent_node(
            "Plan",
            agent,
            |request: &PicnicRequest, _| {
                vec![
                    Message::system(
                        "You plan picnics. Always call weather.forecast and parks.search before you answer. \
                         Go ahead only if the forecast is dry. Then answer with only a JSON object with the fields \
                         city, day, goAhead (true or false), park (one park from parks.search, or null) and reason.",
                    ),
                    Message::user(format!(
                        "Can I have a picnic in {} on {}?",
                        request.city, request.day
                    )),
                ]
            },
            |request: PicnicRequest, result: AgentResult<PicnicPlan>| PicnicRequest {
                tools_used: result.tool_calls.iter().map(|call| call.name.clone()).collect(),
                turns: result.turns,
                plan: Some(result.output),
                ..request
            },
        )
        .add_node("Announce", |request: PicnicRequest| {
            let plan = request.plan.clone().expect("Plan ran first");
            let message = if plan.go_ahead {
                format!(
                    "Picnic on: {}, {}. {}",
                    plan.park.unwrap_or_default(),
                    request.day,
                    plan.reason
                )
            } else {
                format!("No picnic {}. {}", request.day, plan.reason)
            };
            PicnicRequest {
                message: Some(message),
                ..request
            }
        })
        .add_edge("Plan", "Announce")
        .set_recovery_policy("Plan", RecoveryPolicy::retry(1))
        .compile()
        .expect("the graph is valid");
    // docs:end tutorials/llm-agent/graph

    // docs:begin tutorials/llm-agent/run
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let request = PicnicRequest {
        city: "Lisbon".to_string(),
        day: "Saturday".to_string(),
        ..Default::default()
    };

    match graph.run("picnic-1", request, store).await {
        GraphExecutionResult::Completed(done) => {
            println!("tools used: {}", done.tools_used.join(", "));
            println!("model turns: {}", done.turns);
            println!("{}", done.message.unwrap_or_default());
        }
        GraphExecutionResult::Failed(error) => println!("The run failed: {error:?}"),
        _ => println!("The run did not complete."),
    }
    // docs:end tutorials/llm-agent/run
}

#[test]
fn graph_and_run_prints_the_plan() {
    let stdout = run_alone("tutorials::llm_agent::graph_and_run", &[], &[]);
    let expected = "[tool] weather.forecast(Lisbon, Saturday)\n\
                    [tool] parks.search(Lisbon)\n\
                    tools used: weather.forecast, parks.search\n\
                    model turns: 3\n\
                    Picnic on: Jardim da Estrela, Saturday. Sunny and 24°C, with a 5% chance of rain.\n";
    assert!(stdout.contains(expected), "the run printed:\n{stdout}");
}

/// The hosted clients read their keys from the environment, so this builds them only when the keys are set:
/// `hosted_clients_from_the_environment` runs it with made-up keys. Building a client calls nothing.
#[test]
#[allow(unused_variables)]
fn hosted() {
    let names = ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL"];
    if names.iter().any(|name| std::env::var_os(name).is_none()) {
        return;
    }

    // docs:begin tutorials/llm-agent/hosted
    // OpenAI
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is set");
    let chat: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(
        OpenAiOptions::new(api_key).with_model("gpt-4o-mini"),
    ));

    // Anthropic: set ANTHROPIC_MODEL to the model you want.
    let api_key = std::env::var("ANTHROPIC_API_KEY").expect("ANTHROPIC_API_KEY is set");
    let model = std::env::var("ANTHROPIC_MODEL").expect("ANTHROPIC_MODEL is set");
    let chat: Arc<dyn ChatClient> = Arc::new(AnthropicChatClient::new(
        AnthropicOptions::new(api_key).with_model(model),
    ));

    // Another OpenAI-compatible server, such as vLLM or LM Studio
    let chat: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(
        OpenAiOptions::compatible("http://localhost:8000/v1").with_model("my-model"),
    ));
    // docs:end tutorials/llm-agent/hosted

    assert_eq!(chat.provider(), "openai");
    assert_eq!(chat.default_model(), Some("my-model"));
    println!("built the hosted clients");
}

#[test]
fn hosted_clients_from_the_environment() {
    let stdout = run_alone(
        "tutorials::llm_agent::hosted",
        &[],
        &[
            ("OPENAI_API_KEY", "sk-not-a-real-key"),
            ("ANTHROPIC_API_KEY", "sk-ant-not-a-real-key"),
            ("ANTHROPIC_MODEL", "claude-model"),
        ],
    );
    assert!(stdout.contains("built the hosted clients"), "{stdout}");
}
