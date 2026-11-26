//! `inventory-mcp [--warehouse <name>]`: the inventory MCP server over stdio
//! (`server.rs`), one JSON-RPC message per line. It writes one line to stderr
//! first, which a client's log shows, saying whether `INVENTORY_API_KEY`
//! reached it. Not a sample: the server the samples connect to.

mod server;

use std::io::{BufRead, Write};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let warehouse = args
        .iter()
        .position(|arg| arg == "--warehouse")
        .and_then(|at| args.get(at + 1))
        .map_or("east", String::as_str);
    let server = server::InventoryServer::new(warehouse);
    let key = match std::env::var("INVENTORY_API_KEY") {
        Ok(key) if !key.is_empty() => "an API key",
        _ => "no API key",
    };
    eprintln!("inventory-mcp: serving {}, with {key}", server.warehouse);

    let mut stdout = std::io::stdout().lock();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str(&line) else {
            continue;
        };
        if let Some(answer) = server.answer(&message)
            && writeln!(stdout, "{answer}")
                .and_then(|()| stdout.flush())
                .is_err()
        {
            break;
        }
    }
}
