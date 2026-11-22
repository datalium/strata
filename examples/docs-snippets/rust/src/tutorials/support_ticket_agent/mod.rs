//! Samples for <https://stratamesh.dev/tutorials/support-ticket-agent/>.
//!
//! The tutorial builds `src/main.rs` a step at a time. Each module is the program as it stands after one
//! step, with the samples that step adds, and `program` is the program the page ends with. The tests run
//! each one in a child process, in a directory of its own for `tickets.db`, and check what it prints
//! against the page.

mod program;
mod step4;
mod step5;
mod step7;
mod step8;

use crate::child::{self, TempDir};

const REPLY: &str = "Thanks for writing in. Your ticket is with our billing team, and they will reply within one business day.";

fn first_run() -> String {
    format!("-> Classify\n-> DraftReply\n{REPLY}\nPaused after DraftReply, before SendReply.")
}

fn approval() -> String {
    format!(
        "Review T-1001 (billing): {REPLY}\nCompleted: category billing, status sent (approved by dana@example.com)"
    )
}

fn run(step: &str, arg: &str, dir: &TempDir) -> String {
    child::run(
        &format!("tutorials::support_ticket_agent::{step}::main_runs"),
        arg,
        dir.path(),
    )
}

#[test]
fn step5_classifies_the_ticket() {
    let dir = TempDir::new("ticket-agent-step5");
    assert_eq!(
        run("step5", "run", &dir),
        "-> Classify\n\nCompleted: category billing, status open"
    );
}

#[test]
fn step7_pauses_after_the_draft() {
    let dir = TempDir::new("ticket-agent-step7");
    assert_eq!(run("step7", "run", &dir), first_run());
}

#[test]
fn step8_approves_after_a_restart() {
    let dir = TempDir::new("ticket-agent-step8");
    assert_eq!(run("step8", "run", &dir), first_run());
    assert_eq!(run("step8", "approve", &dir), approval());

    // A waiting thread can be answered once: the draft still shows, and the resume is refused.
    assert_eq!(
        run("step8", "approve", &dir),
        format!(
            "Review T-1001 (billing): {REPLY}\nFailed: checkpoint for thread 'T-1001' was already resumed"
        )
    );
}

#[test]
fn the_full_program_prints_what_the_page_shows() {
    let dir = TempDir::new("ticket-agent");
    assert_eq!(run("program", "run", &dir), first_run());
    assert_eq!(
        run("program", "approve", &dir),
        format!(
            "{}\n[User] My invoice total looks wrong this month\n[Assistant] {REPLY}",
            approval()
        )
    );
}
