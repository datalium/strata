//! The scripted MCP server of `tests/support/server.rs` over stdin and
//! stdout, for `datalium-strata-mcp`'s stdio tests. Not a sample.
//!
//! `mcp_fixture_server <era> [--stubborn] [--child <pid file>] [--page-size <n>] [--stderr <line>]`
//! `mcp_fixture_server --sleeper`: a child that only waits, for the
//! process-tree tests.

#[path = "../tests/support/server.rs"]
mod server;

use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--sleeper") {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
    let mut script = server::Script::new(server::Era::parse(
        args.first().map(String::as_str).unwrap_or("modern"),
    ));
    let mut rest = args.iter().skip(1);
    // Kept alive while serving: a child of this server's own.
    let mut _child = None;
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--stubborn" => script.stubborn = true,
            "--page-size" => script.page_size = rest.next().unwrap().parse().unwrap(),
            "--stderr" => eprintln!("{}", rest.next().unwrap()),
            "--child" => {
                let pid_file = rest.next().unwrap();
                let child = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--sleeper")
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .spawn()
                    .unwrap();
                std::fs::write(pid_file, format!("{} {}", std::process::id(), child.id())).unwrap();
                _child = Some(child);
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let ended = server::serve(script, tokio::io::stdin(), tokio::io::stdout()).await;
    if ended == server::Ended::Crashed {
        std::process::exit(3);
    }
}
