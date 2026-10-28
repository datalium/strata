//! `strata graph view` / `strata graph validate` end-to-end, invoking the
//! actual compiled binary as a subprocess.

use std::process::Command;

fn strata() -> Command {
    Command::new(env!("CARGO_BIN_EXE_strata"))
}

fn fixture_path(name: &str) -> String {
    format!("{}/../schema/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn graph_validate_accepts_a_valid_graph() {
    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(fixture_path("graph-validation/valid.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("OK:"));
}

#[test]
fn graph_validate_rejects_an_unguarded_cycle_and_reports_it_as_json() {
    let output = strata()
        .args(["graph", "validate", "--json", "--path"])
        .arg(fixture_path("graph-validation/unguarded-cycle.json"))
        .output()
        .unwrap();

    assert!(!output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["valid"], false);
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_UNGUARDED_CYCLE");
}

#[test]
fn graph_validate_accepts_two_paths_rejoining_inside_a_guarded_loop() {
    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(fixture_path("graph-validation/guarded-loop-rejoin.json"))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("OK:"));
}

#[test]
fn graph_validate_rejects_an_edge_to_a_name_that_is_not_a_node() {
    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(fixture_path("graph-validation/dangling-edge-target.json"))
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[STRATA_ERR_DANGLING_EDGE] B -> Missing:"));
}

#[test]
fn graph_view_renders_every_node_name_and_the_guarded_retry_edge() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-definition.sample.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("PrepareWireTransfer"));
    assert!(stdout.contains("ValidateApproval"));
    assert!(stdout.contains("ExecuteWireTransfer"));
    assert!(stdout.contains("ValidateApproval -> PrepareWireTransfer"));
    assert!(stdout.contains("retry_count < 3"));
}

#[test]
fn graph_view_json_mode_emits_the_raw_graph_definition() {
    let output = strata()
        .args(["graph", "view", "--json", "--path"])
        .arg(fixture_path("graph-definition.sample.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["entry_point"], "PrepareWireTransfer");
    assert_eq!(json["nodes"].as_array().unwrap().len(), 3);
}

/// A temp directory holding copies of the named `schema/fixtures/graph-validation/`
/// files, each at the given relative path.
fn graph_dir(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (relative, fixture) in files {
        let target = dir.path().join(relative);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(fixture_path(&format!("graph-validation/{fixture}")), target).unwrap();
    }
    dir
}

fn validate_dir_json(dir: &std::path::Path) -> (std::process::ExitStatus, serde_json::Value) {
    let output = strata()
        .args(["graph", "validate", "--json", "--path"])
        .arg(dir)
        .output()
        .unwrap();
    let json = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout wasn't JSON ({e}): {}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    (output.status, json)
}

#[test]
fn graph_validate_directory_accepts_when_every_graph_is_valid() {
    let dir = graph_dir(&[("a.json", "valid.json"), ("nested/b.json", "valid.json")]);

    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(dir.path())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("2 graph file(s) checked: 2 valid, 0 invalid, 0 unreadable."));
}

#[test]
fn graph_validate_directory_reports_every_file_and_fails_if_any_is_invalid() {
    let dir = graph_dir(&[
        ("b-cycle.json", "unguarded-cycle.json"),
        ("a-ok.json", "valid.json"),
        ("nested/c-orphan.json", "orphan-node.json"),
    ]);

    let (status, json) = validate_dir_json(dir.path());

    assert_eq!(status.code(), Some(1));
    assert_eq!(json["valid"], false);
    assert_eq!(json["checked"], 3);
    assert_eq!(json["valid_count"], 1);
    assert_eq!(json["invalid_count"], 2);
    assert_eq!(json["unreadable_count"], 0);

    // Path order, recursing into sub-directories, one entry per file.
    let graphs = json["graphs"].as_array().unwrap();
    let names: Vec<&str> = graphs
        .iter()
        .map(|g| {
            std::path::Path::new(g["path"].as_str().unwrap())
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect();
    assert_eq!(names, ["a-ok.json", "b-cycle.json", "c-orphan.json"]);
    assert_eq!(graphs[0]["valid"], true);
    assert_eq!(graphs[1]["errors"][0]["code"], "STRATA_ERR_UNGUARDED_CYCLE");
    assert_eq!(graphs[2]["errors"][0]["code"], "STRATA_ERR_ORPHAN");
}

#[test]
fn graph_validate_directory_human_output_names_each_failing_file_and_error() {
    let dir = graph_dir(&[("ok.json", "valid.json"), ("bad.json", "orphan-node.json")]);

    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(dir.path())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("FAILED"));
    assert!(stdout.contains("bad.json"));
    assert!(stdout.contains("[STRATA_ERR_ORPHAN] B:"));
    assert!(stdout.contains("2 graph file(s) checked: 1 valid, 1 invalid, 0 unreadable."));
}

#[test]
fn graph_validate_directory_treats_an_unreadable_file_as_worse_than_an_invalid_one() {
    let dir = graph_dir(&[("cycle.json", "unguarded-cycle.json")]);
    std::fs::write(dir.path().join("not-a-graph.json"), r#"{"hello": "world"}"#).unwrap();

    let (status, json) = validate_dir_json(dir.path());

    assert_eq!(status.code(), Some(2));
    assert_eq!(json["valid"], false);
    assert_eq!(json["invalid_count"], 1);
    assert_eq!(json["unreadable_count"], 1);
    let unreadable = json["graphs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["path"].as_str().unwrap().ends_with("not-a-graph.json"))
        .unwrap();
    assert!(
        unreadable["error"]
            .as_str()
            .unwrap()
            .contains("as a graph definition")
    );
}

#[test]
fn graph_validate_directory_ignores_non_json_files() {
    let dir = graph_dir(&[("ok.json", "valid.json")]);
    std::fs::write(dir.path().join("README.md"), "# graphs").unwrap();
    std::fs::write(dir.path().join("graph.json.bak"), "not json at all").unwrap();

    let (status, json) = validate_dir_json(dir.path());

    assert_eq!(status.code(), Some(0));
    assert_eq!(json["checked"], 1);
}

#[test]
fn graph_validate_directory_with_no_graphs_is_an_error_not_a_pass() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("README.md"), "# nothing here").unwrap();

    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(dir.path())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("no graph definition files"));
}

// --- Graph structure (design/22-Graph-Structure-In-Definitions.md) ----------

fn validate_json(path: &str) -> (Option<i32>, serde_json::Value) {
    let output = strata()
        .args(["graph", "validate", "--json", "--path", path])
        .output()
        .unwrap();
    (
        output.status.code(),
        serde_json::from_slice(&output.stdout).unwrap(),
    )
}

#[test]
fn graph_validate_accepts_a_hierarchy_that_is_valid_at_every_level() {
    let (code, json) = validate_json(&fixture_path("graph-validation/valid-hierarchy.json"));

    assert_eq!(code, Some(0), "{json}");
    assert_eq!(json["valid"], true);
}

#[test]
fn graph_validate_names_a_child_error_by_its_qualified_path() {
    let path = fixture_path("graph-validation/child-orphan.json");
    let output = strata()
        .args(["graph", "validate", "--path", &path])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[STRATA_ERR_ORPHAN] Remediate/Abort: Node 'Remediate/Abort' is isolated"),
        "{stdout}"
    );

    let (code, json) = validate_json(&path);
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"].as_array().unwrap().len(), 1);
    assert_eq!(json["errors"][0]["kind"], "OrphanNode");
    assert_eq!(json["errors"][0]["node"], "Remediate/Abort");
}

#[test]
fn graph_validate_qualifies_an_error_two_levels_down() {
    let (code, json) = validate_json(&fixture_path(
        "graph-validation/grandchild-unguarded-cycle.json",
    ));

    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_UNGUARDED_CYCLE");
    assert_eq!(json["errors"][0]["node"], "Remediate/Fix/Verify");
    assert_eq!(json["errors"][0]["related_node"], "Remediate/Fix/Apply");
}

#[test]
fn graph_validate_rejects_an_empty_group_and_a_duplicate_branch() {
    let (code, json) = validate_json(&fixture_path("graph-validation/empty-group.json"));
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_EMPTY_GROUP");
    assert_eq!(json["errors"][0]["kind"], "EmptyGroup");
    assert_eq!(json["errors"][0]["node"], "Investigate");

    let path = fixture_path("graph-validation/duplicate-branch.json");
    let (code, json) = validate_json(&path);
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_DUPLICATE_BRANCH");
    assert_eq!(json["errors"][0]["related_node"], "Kubernetes");

    // The human line names the branch by its path, not as an edge.
    let output = strata()
        .args(["graph", "validate", "--path", &path])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[STRATA_ERR_DUPLICATE_BRANCH] Investigate/Kubernetes:"),
        "{stdout}"
    );
}

#[test]
fn graph_validate_reads_joins_and_rejects_an_invalid_quorum_at_every_level() {
    // design/26-Parallel-Group-Controls-And-Joins.md
    let (code, json) = validate_json(&fixture_path("graph-export/joins.json"));
    assert_eq!(code, Some(0), "{json}");

    let (code, json) = validate_json(&fixture_path("graph-validation/invalid-quorum.json"));
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_INVALID_QUORUM");
    assert_eq!(json["errors"][0]["node"], "Vote");
    assert_eq!(json["errors"][1]["code"], "STRATA_ERR_INVALID_QUORUM");
    assert_eq!(json["errors"][1]["node"], "Review/Ask");
}

#[test]
fn graph_validate_refuses_a_join_below_1_2_0() {
    let mut joins: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path("graph-export/joins.json")).unwrap(),
    )
    .unwrap();
    joins["schema_version"] = "1.1.0".into();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.json");
    std::fs::write(&path, serde_json::to_string(&joins).unwrap()).unwrap();

    let output = strata()
        .args(["graph", "validate", "--path"])
        .arg(&path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node 'Specialists' has a join, which schema_version 1.1.0 doesn't have"),
        "{stderr}"
    );
}

#[test]
fn graph_validate_reports_every_level_in_order() {
    let (_, json) = validate_json(&fixture_path("graph-validation/errors-at-every-level.json"));
    let nodes: Vec<&str> = json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["node"].as_str().unwrap())
        .collect();

    assert_eq!(nodes, ["Island", "G", "S/Q", "S/H"]);
}

#[test]
fn graph_validate_directory_checks_every_level_of_each_hierarchical_file() {
    let dir = graph_dir(&[
        ("good.json", "valid-hierarchy.json"),
        ("bad.json", "child-orphan.json"),
    ]);

    let (status, json) = validate_dir_json(dir.path());

    assert_eq!(status.code(), Some(1));
    assert_eq!(json["valid_count"], 1);
    assert_eq!(json["invalid_count"], 1);
    let bad = &json["graphs"][0];
    assert!(bad["path"].as_str().unwrap().ends_with("bad.json"));
    assert_eq!(bad["errors"][0]["node"], "Remediate/Abort");
}

/// Writes `value` to a temp file and runs `graph <command> --path` on it.
fn run_on(command: &str, value: &serde_json::Value) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.json");
    std::fs::write(&path, serde_json::to_string_pretty(value).unwrap()).unwrap();
    strata()
        .args(["graph", command, "--path"])
        .arg(&path)
        .output()
        .unwrap()
}

fn hierarchy() -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture_path("graph-export/hierarchy.json")).unwrap(),
    )
    .unwrap()
}

#[test]
fn graph_validate_and_view_refuse_a_schema_version_they_do_not_know() {
    let mut future = hierarchy();
    future["nodes"][2]["graph"]["schema_version"] = "1.9.0".into();

    for command in ["validate", "view"] {
        let output = run_on(command, &future);
        assert_eq!(output.status.code(), Some(2), "{command}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("unsupported schema_version '1.9.0' (in 'Remediate')"),
            "{command}: {stderr}"
        );
    }
}

#[test]
fn graph_validate_refuses_a_kind_that_does_not_match_its_fields() {
    let mut no_branches = hierarchy();
    no_branches["nodes"][1]
        .as_object_mut()
        .unwrap()
        .remove("branches");
    let output = run_on("validate", &no_branches);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("node 'Investigate' is a parallel_group but has neither branches nor graph")
    );

    let mut stray_graph = hierarchy();
    stray_graph["nodes"][0]["graph"] = hierarchy()["nodes"][2]["graph"].clone();
    assert_eq!(run_on("validate", &stray_graph).status.code(), Some(2));

    let mut old_version = hierarchy();
    old_version["schema_version"] = "1.0.0".into();
    assert_eq!(run_on("validate", &old_version).status.code(), Some(2));

    let mut unknown_kind = hierarchy();
    unknown_kind["nodes"][1]["kind"] = "loop".into();
    assert_eq!(run_on("validate", &unknown_kind).status.code(), Some(2));
}

fn map_fixture() -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(fixture_path("graph-export/map.json")).unwrap())
        .unwrap()
}

#[test]
fn graph_validate_reads_maps_and_names_template_nodes_with_a_star() {
    // design/27-Map-Fan-Out.md
    let (code, json) = validate_json(&fixture_path("graph-export/map.json"));
    assert_eq!(code, Some(0), "{json}");

    let (code, json) = validate_json(&fixture_path("graph-validation/map-template-errors.json"));
    assert_eq!(code, Some(1));
    let errors: Vec<(&str, &str)> = json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (e["code"].as_str().unwrap(), e["node"].as_str().unwrap()))
        .collect();
    assert_eq!(
        errors,
        [
            ("STRATA_ERR_INVALID_QUORUM", "Vote"),
            ("STRATA_ERR_ORPHAN", "Investigate/*/Stray"),
            ("STRATA_ERR_UNGUARDED_CYCLE", "Review/Fan/*/Loop"),
        ]
    );
}

#[test]
fn graph_validate_refuses_a_map_below_1_3_0() {
    let mut old = map_fixture();
    old["schema_version"] = "1.2.0".into();

    let output = run_on("validate", &old);

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node 'Investigate' is a map, which schema_version 1.2.0 doesn't have"),
        "{stderr}"
    );
}

#[test]
fn graph_view_annotates_a_map() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/map.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in [
        "  Investigate: map over a template, entry Assess, 3 nodes\n",
        "  Confirm: map over a template, entry Check, 1 node, join first_matching (confidence >= 0.9), keep_succeeded\n",
        "  Notify: parallel group [email, chat], join first_success, fail_group\n",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in {stdout}");
    }
}

#[test]
fn graph_validate_directory_counts_a_malformed_hierarchy_as_unreadable() {
    let dir = graph_dir(&[("good.json", "valid-hierarchy.json")]);
    let mut broken = hierarchy();
    broken["nodes"][2]["kind"] = "parallel_group".into();
    std::fs::write(
        dir.path().join("broken.json"),
        serde_json::to_string(&broken).unwrap(),
    )
    .unwrap();

    let (status, json) = validate_dir_json(dir.path());

    assert_eq!(status.code(), Some(2));
    assert_eq!(json["unreadable_count"], 1);
    assert_eq!(json["valid_count"], 1);
}

#[test]
fn graph_view_annotates_a_groups_join() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/joins.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in [
        "  Specialists: parallel group [telemetry, kubernetes, database], join all, keep_succeeded\n",
        "  Race: parallel group [openai, anthropic, local], join first_matching (validated, confidence >= 0.9), keep_succeeded\n",
        "  Fastest: parallel group [primary, secondary], join first_success, fail_group\n",
        "  Majority: parallel group [openai, anthropic, local], join quorum 2, fail_group\n",
        "  Plain: parallel group [a, b]\n",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in {stdout}");
    }
}

#[test]
fn graph_view_annotates_each_composite_node() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/hierarchy.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Composite nodes:"), "{stdout}");
    assert!(
        stdout.contains("  Investigate: parallel group [Kubernetes, Prometheus, GitHub]\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  Remediate: subgraph, entry Plan, 4 nodes\n"),
        "{stdout}"
    );
}

#[test]
fn graph_view_json_mode_prints_the_whole_hierarchy() {
    let output = strata()
        .args(["graph", "view", "--json", "--path"])
        .arg(fixture_path("graph-export/hierarchy.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json, hierarchy());
}

#[test]
fn graph_view_of_a_plain_graph_has_no_composite_section() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-definition.sample.json"))
        .output()
        .unwrap();

    assert!(!String::from_utf8_lossy(&output.stdout).contains("Composite nodes:"));
}

// --- Waits (design/29-Event-Waits-And-Timers.md) ---------------------------------

fn waits_fixture() -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(fixture_path("graph-export/waits.json")).unwrap())
        .unwrap()
}

#[test]
fn graph_validate_reads_waits_and_counts_a_timeout_route_as_an_edge() {
    let (code, json) = validate_json(&fixture_path("graph-export/waits.json"));
    assert_eq!(code, Some(0), "{json}");

    // Without its on_timeout edges, Escalate and Rollback are orphans.
    let mut without = waits_fixture();
    without["edges"]
        .as_array_mut()
        .unwrap()
        .retain(|edge| edge["condition"] != "on_timeout");
    let output = run_on("validate", &without);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[STRATA_ERR_ORPHAN] Escalate"), "{stdout}");
    assert!(stdout.contains("[STRATA_ERR_ORPHAN] Rollback"), "{stdout}");
}

#[test]
fn graph_validate_reports_a_wait_in_a_maps_template_at_any_depth() {
    let (code, json) = validate_json(&fixture_path("graph-validation/wait-in-map.json"));

    assert_eq!(code, Some(1), "{json}");
    let errors: Vec<(String, String)> = json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|error| {
            (
                error["code"].as_str().unwrap().to_string(),
                error["node"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        errors,
        [
            (
                "STRATA_ERR_WAIT_IN_MAP".to_string(),
                "Fan/*/Window".to_string()
            ),
            (
                "STRATA_ERR_WAIT_IN_MAP".to_string(),
                "Review/Batch/*/Inner/Hold".to_string()
            ),
        ]
    );
}

#[test]
fn graph_validate_refuses_a_wait_below_1_4_0_or_without_its_wait() {
    let mut old = waits_fixture();
    old["schema_version"] = "1.3.0".into();
    // 1.3.0 had no boundaries either (design/48-Boundaries-In-Definitions.md).
    old["nodes"][1].as_object_mut().unwrap().remove("interrupt");
    let output = run_on("validate", &old);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node 'Deploying' is a wait, which schema_version 1.3.0 doesn't have"),
        "{stderr}"
    );

    let mut no_wait = waits_fixture();
    no_wait["nodes"][4].as_object_mut().unwrap().remove("wait");
    let output = run_on("validate", &no_wait);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node 'Window' is a wait but has no wait"),
        "{stderr}"
    );
}

#[test]
fn graph_view_annotates_a_wait() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/waits.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in [
        "  Deploying: wait for event deployment.finished, times out after 1h\n",
        "  Window: timer, 5m\n",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in {stdout}");
    }
}

// --- Boundaries (design/48-Boundaries-In-Definitions.md) ------------------------

fn boundaries_fixture() -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture_path("graph-export/boundaries.json")).unwrap(),
    )
    .unwrap()
}

#[test]
fn graph_validate_reads_boundaries() {
    let (code, json) = validate_json(&fixture_path("graph-export/boundaries.json"));
    assert_eq!(code, Some(0), "{json}");
}

#[test]
fn graph_validate_reports_a_boundary_in_a_maps_template_at_any_depth() {
    let (code, json) = validate_json(&fixture_path("graph-validation/boundary-in-map.json"));

    assert_eq!(code, Some(1), "{json}");
    let errors: Vec<(&str, &str)> = json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|error| {
            (
                error["code"].as_str().unwrap(),
                error["node"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        errors,
        [
            ("STRATA_ERR_WAIT_IN_MAP", "Fan/*/Window"),
            ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Read"),
            ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Inner/Sign"),
        ]
    );
}

#[test]
fn graph_validate_refuses_a_misplaced_interrupt_or_route_with_exit_2() {
    let refused = |value: serde_json::Value, message: &str| {
        let output = run_on("validate", &value);
        assert_eq!(output.status.code(), Some(2), "{message}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
    };

    let mut old = boundaries_fixture();
    old["schema_version"] = "1.5.0".into();
    refused(
        old,
        "node 'Approve' has an interrupt, which schema_version 1.5.0 doesn't have",
    );

    let mut on_wait = waits_fixture();
    on_wait["nodes"][2]["interrupt"] = serde_json::json!({ "timeout_ms": null });
    refused(
        on_wait,
        "node 'Deploying' is a wait and can't also have an interrupt",
    );

    let mut unmarked = boundaries_fixture();
    unmarked["nodes"][1]
        .as_object_mut()
        .unwrap()
        .remove("interrupt");
    refused(
        unmarked,
        "node 'Approve' has an on_reject edge but is not an interrupt boundary",
    );

    let mut untimed = boundaries_fixture();
    untimed["nodes"][1]["interrupt"]["timeout_ms"] = serde_json::Value::Null;
    refused(
        untimed,
        "node 'Approve' has an on_timeout edge but no timeout",
    );
}

#[test]
fn graph_view_annotates_each_boundary() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/boundaries.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in [
        "  Approve: interrupt boundary, rejects to Abort, times out after 30m to Escalate
",
        "  Gather: parallel group [Logs, Metrics]; interrupt boundary
",
        "  Review: subgraph, entry Draft, 2 nodes; interrupt boundary, times out after 1h, then the thread ends
",
        "  Fan: map over a template, entry Check, 1 node; interrupt boundary, times out after 5m, then the thread ends
",
    ] {
        assert!(stdout.contains(line), "missing {line:?} in {stdout}");
    }

    // A boundary with only a reject route.
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/reject-route.json"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(
            "  Approve: interrupt boundary, rejects to Abort
"
        ),
        "{stdout}"
    );
}

// --- Graph identity (design/31-Graph-Identity.md) ---------------------------------

fn identity_fixture() -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture_path("graph-export/identity.json")).unwrap(),
    )
    .unwrap()
}

#[test]
fn graph_validate_reads_an_identity_and_refuses_one_below_1_5_0_or_with_an_empty_id() {
    let (code, json) = validate_json(&fixture_path("graph-export/identity.json"));
    assert_eq!(code, Some(0), "{json}");

    let mut old = identity_fixture();
    old["schema_version"] = "1.4.0".into();
    let output = run_on("validate", &old);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the definition has an identity, which schema_version 1.4.0 doesn't have"),
        "{stderr}"
    );

    let mut blank = identity_fixture();
    blank["nodes"][1]["graph"]["identity"]["id"] = "".into();
    let output = run_on("validate", &blank);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the definition (in 'Remediate') has an identity with an empty id"),
        "{stderr}"
    );
}

#[test]
fn graph_view_names_the_graph_and_each_childs() {
    let output = strata()
        .args(["graph", "view", "--path"])
        .arg(fixture_path("graph-export/identity.json"))
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with("Graph: incident version 1\n\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  Remediate: subgraph remediation unversioned, entry Apply, 2 nodes\n"),
        "{stdout}"
    );
}
