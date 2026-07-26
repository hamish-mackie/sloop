//! Integration coverage for panel result checks, driven end to end by
//! scripted fake reviewers.
//!
//! The aggregation itself is unit-tested in `src/flow.rs` without any process
//! at all; what these tests prove is the part a pure function cannot — that
//! real reviewer processes get credentials scoped to their own seat, that
//! their reports land on the `(stage, attempt, reviewer)` row the credential
//! names, and that the verdict the walk acts on is the one those rows imply.

mod support;

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use support::{World, wait_until_slow};

const PANEL_PROMPT: &str = "Judge the work on this branch.\n";

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn sloop_binary() -> String {
    shell_quote(env!("CARGO_BIN_EXE_sloop"))
}

fn write_script(world: &World, name: &str, body: &str) -> PathBuf {
    let path = world.root().join(name);
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}")).expect("write script");
    path
}

/// A reviewer that reports once and exits cleanly, saving the daemon's reply
/// where the test can read it. The exit is deliberately 0 whatever it
/// concluded — a reviewer that failed the work is not a reviewer that failed.
fn reviewer(world: &World, name: &str, verdict: &str, reason: &str, confidence: &str) -> PathBuf {
    write_script(
        world,
        &format!("{name}.sh"),
        &format!(
            "{sloop} --json verdict {verdict} --reason {reason} --confidence {confidence} \
             > {name}.out 2> {name}.err || true\nexit 0\n",
            sloop = sloop_binary(),
            reason = shell_quote(reason),
        ),
    )
}

/// A reviewer that exits without reporting anything at all.
fn silent_reviewer(world: &World, name: &str) -> PathBuf {
    write_script(world, &format!("{name}.sh"), "exit 0\n")
}

/// The build agent every panel scenario opens with: one commit, so the run has
/// something real to merge.
fn builder(world: &World) -> PathBuf {
    write_script(
        world,
        "builder.sh",
        "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet \
         --allow-empty -m build\nexit 0\n",
    )
}

/// Writes the config, the flow, and the panel prompt file. `targets` is the
/// `(name, script)` pairs the flow's reviewers name.
fn configure(world: &World, flow: &str, targets: &[(&str, &Path)]) {
    let sloop_dir = world.root().join(".agents/sloop");
    fs::create_dir_all(sloop_dir.join("flows")).expect("create flow directory");
    fs::create_dir_all(sloop_dir.join("prompts")).expect("create prompt directory");
    fs::write(sloop_dir.join("prompts/panel.md"), PANEL_PROMPT).expect("write panel prompt");
    fs::write(sloop_dir.join("flows/default.yaml"), flow).expect("write flow");

    let mut config = String::from(
        "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: builder\n  targets:\n",
    );
    for (name, script) in targets {
        config.push_str(&format!(
            "    {name}:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).expect("serialize script path"),
        ));
    }
    fs::write(sloop_dir.join("config.yaml"), config).expect("write config");
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Panel scenario\n");
    let output = world.sloop(&["post", ticket.to_str().expect("UTF-8 path"), "--manual"]);
    assert!(
        output.status.success(),
        "post failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .expect("ticket id")
        .to_owned()
}

fn status(world: &World) -> Value {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"].clone()
}

fn settled(world: &World) -> bool {
    let tickets = &status(world)["tickets"];
    tickets["merged"].as_i64().unwrap_or(0)
        + tickets["needs_review"].as_i64().unwrap_or(0)
        + tickets["failed"].as_i64().unwrap_or(0)
        > 0
}

/// Every `panel_report` row for a run, in evidence order, as the raw JSON the
/// daemon persisted. Reading the rows rather than the rendered aggregate is
/// deliberate: the aggregate is derived, so the rows are the only thing there
/// is to be right about.
fn panel_reports(world: &World, run_id: &str) -> Vec<Value> {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let mut statement = connection
        .prepare(
            "SELECT data_json FROM run_evidence
             WHERE run_id = ?1 AND kind = 'panel_report' ORDER BY sequence",
        )
        .expect("prepare panel report query");
    statement
        .query_map([run_id], |row| row.get::<_, String>(0))
        .expect("query panel reports")
        .map(|data| serde_json::from_str(&data.expect("read report JSON")).expect("report is JSON"))
        .collect()
}

/// The `show` row for one named stage of a run.
fn shown_stage(world: &World, run: &str, stage: &str) -> Value {
    world.show_snapshot(run)["stages"]
        .as_array()
        .expect("stages array")
        .iter()
        .find(|row| row["stage"] == stage)
        .unwrap_or_else(|| panic!("no `{stage}` stage in the run's show output"))
        .clone()
}

fn worktree_json(world: &World, name: &str) -> Value {
    let path = world.run_worktree(1).join(name);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("{name} is not JSON: {error}\n{text}"))
}

/// A three-seat panel over a trivially passing action, with the stage's fail
/// action spelled out by the caller.
fn three_seat_flow(quorum: u32, fail_action: &str) -> String {
    format!(
        "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n\
         - name: review\n  \
           action: {{ exec: ['true'] }}\n  \
           result_check:\n    \
             panel:\n      \
               prompt: prompts/panel.md\n      \
               reviewers: [{{ target: alpha }}, {{ target: beta }}, {{ target: gamma }}]\n      \
               require: {{ quorum: {quorum} }}\n  \
           fail_action: {fail_action}\n\
         - {{ name: merge, action: {{ builtin: merge }}, result_check: none }}\n"
    )
}

/// The quorum is met, so the stage passes and the walk carries on to the
/// merge. The dissenting reviewer is still recorded: a panel that agreed and
/// one that was outvoted must not read the same afterwards.
#[test]
fn a_two_of_three_quorum_passes_and_the_run_proceeds() {
    let world = World::configured();
    let build = builder(&world);
    let alpha = reviewer(&world, "alpha", "pass", "reads correct", "high");
    let beta = reviewer(&world, "beta", "pass", "tests cover it", "medium");
    let gamma = reviewer(&world, "gamma", "fail", "naming could be better", "low");
    configure(
        &world,
        &three_seat_flow(2, "fail"),
        &[
            ("builder", &build),
            ("alpha", &alpha),
            ("beta", &beta),
            ("gamma", &gamma),
        ],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-quorum-met.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the panel-approved run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let run = world.run_id(1);
    let reports = panel_reports(&world, &run);
    assert_eq!(reports.len(), 3, "{reports:?}");
    for (seat, expected) in [(0, "pass"), (1, "pass"), (2, "fail")] {
        let report = reports
            .iter()
            .find(|report| report["reviewer"] == seat)
            .unwrap_or_else(|| panic!("no report for seat {seat}: {reports:?}"));
        assert_eq!(report["verdict"], expected, "{report}");
        assert_eq!(report["stage"], "review");
        assert_eq!(report["attempt"], 1);
    }
    assert_eq!(reports[2]["confidence"], "low");
    assert_eq!(reports[2]["reason"], "naming could be better");

    // The stage's verdict names the panel as its source and carries the tally,
    // not any one reviewer's words.
    let review = shown_stage(&world, &run, "review");
    assert_eq!(review["state"], "passed");
    assert_eq!(review["verdict_source"], "panel");
    assert_eq!(review["reason"], "panel: 2 of 3 reviewers passed, quorum 2");

    // Every seat is listed, dissent included.
    let seats = review["reviewers"].as_array().expect("reviewer rows");
    assert_eq!(seats.len(), 3);
    assert_eq!(seats[0]["target"], "alpha");
    assert_eq!(seats[2]["verdict"], "fail");
    assert_eq!(seats[2]["confidence"], "low");
    assert_eq!(seats[2]["reason"], "naming could be better");

    // And all of it reaches the text an operator actually reads. A tally in
    // the envelope that the renderer drops is a tally nobody sees.
    let text = String::from_utf8_lossy(&world.sloop_plain(&["show", &world.run_alias(1)]).stdout)
        .into_owned();
    for line in [
        "review  passed",
        "verdict from panel",
        "alpha  pass  confidence high  reads correct",
        "beta   pass  confidence medium  tests cover it",
        "gamma  fail  confidence low  naming could be better",
    ] {
        assert!(text.contains(line), "show missed {line:?}:\n{text}");
    }
}

/// One pass out of three is short of a two-vote quorum, so the stage fails —
/// and its `fail_action` decides what that costs. `continue` is advisory, so
/// the walk records the failure and carries on.
#[test]
fn a_one_of_three_panel_fails_the_stage_and_its_fail_action_applies() {
    let world = World::configured();
    let build = builder(&world);
    let alpha = reviewer(&world, "alpha", "pass", "acceptable", "medium");
    let beta = reviewer(&world, "beta", "fail", "missing a test", "high");
    let gamma = reviewer(&world, "gamma", "fail", "leaks a handle", "high");
    configure(
        &world,
        &three_seat_flow(2, "continue"),
        &[
            ("builder", &build),
            ("alpha", &alpha),
            ("beta", &beta),
            ("gamma", &gamma),
        ],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-quorum-missed.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the advisory panel failure settles", || settled(&world));

    let run = world.run_id(1);
    let review = shown_stage(&world, &run, "review");
    assert_eq!(review["state"], "failed");
    assert_eq!(review["verdict_source"], "panel");
    assert_eq!(review["reason"], "panel: 1 of 3 reviewers passed, quorum 2");

    // `continue` is what makes this an advisory failure rather than a halt:
    // the merge after it still ran.
    assert_eq!(shown_stage(&world, &run, "merge")["state"], "passed");
    assert_eq!(status(&world)["tickets"]["merged"], 1);
}

/// A reviewer that exits without reporting has approved nothing. Its seat
/// counts as a `Fail`, and `show` says so in the same words the rest of the
/// system uses for an unreported verdict.
#[test]
fn a_silent_reviewer_counts_as_a_fail() {
    let world = World::configured();
    let build = builder(&world);
    let alpha = reviewer(&world, "alpha", "pass", "reads correct", "high");
    let beta = silent_reviewer(&world, "beta");
    let gamma = reviewer(&world, "gamma", "pass", "tests cover it", "medium");
    configure(
        &world,
        &three_seat_flow(3, "fail"),
        &[
            ("builder", &build),
            ("alpha", &alpha),
            ("beta", &beta),
            ("gamma", &gamma),
        ],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-silent-reviewer.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the silent reviewer fails the stage", || settled(&world));

    let run = world.run_id(1);
    // Only the reviewers that spoke left rows; the silent seat is filled in by
    // the aggregation, never written.
    let reports = panel_reports(&world, &run);
    assert_eq!(reports.len(), 2, "{reports:?}");
    assert!(
        reports.iter().all(|report| report["reviewer"] != 1),
        "{reports:?}"
    );

    let review = shown_stage(&world, &run, "review");
    assert_eq!(review["state"], "failed");
    assert_eq!(review["reason"], "panel: 2 of 3 reviewers passed, quorum 3");
    let seats = review["reviewers"].as_array().expect("reviewer rows");
    assert_eq!(seats[1]["target"], "beta");
    assert_eq!(seats[1]["verdict"], "fail");
    assert_eq!(seats[1]["reason"], "no verdict reported");
    // Nothing was heard from it, so nothing is claimed about how sure it was.
    assert_eq!(seats[1]["confidence"], Value::Null);
    assert_eq!(status(&world)["tickets"]["merged"], 0);
}

/// The credential authorises exactly one report. A reviewer that tries to
/// revise its vote is refused, and the first report is what the panel counts.
#[test]
fn a_reviewers_second_verdict_call_is_denied() {
    let world = World::configured();
    let build = builder(&world);
    let alpha = write_script(
        &world,
        "alpha.sh",
        &format!(
            "{sloop} --json verdict fail --reason 'first word' > alpha.out 2> alpha.err\n\
             {sloop} --json verdict pass --reason 'changed my mind' \
             > second.out 2> second.err || true\n\
             exit 0\n",
            sloop = sloop_binary(),
        ),
    );
    let beta = reviewer(&world, "beta", "pass", "acceptable", "medium");
    let gamma = reviewer(&world, "gamma", "pass", "acceptable", "medium");
    configure(
        &world,
        &three_seat_flow(3, "fail"),
        &[
            ("builder", &build),
            ("alpha", &alpha),
            ("beta", &beta),
            ("gamma", &gamma),
        ],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-one-shot.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the one-shot reviewer settles the run", || settled(&world));

    let first = worktree_json(&world, "alpha.out");
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["data"]["verdict"]["reviewer"], 0);
    assert_eq!(first["data"]["verdict"]["verdict"], "fail");
    // Absent from the call, so recorded as the documented default.
    assert_eq!(first["data"]["verdict"]["confidence"], "medium");

    let second = worktree_json(&world, "second.err");
    assert_eq!(second["ok"], false, "{second}");
    assert_eq!(second["error"]["code"], "conflict");

    // The refused call changed nothing: one row, holding the first word.
    let reports = panel_reports(&world, &world.run_id(1));
    let alpha_reports: Vec<&Value> = reports
        .iter()
        .filter(|report| report["reviewer"] == 0)
        .collect();
    assert_eq!(alpha_reports.len(), 1, "{reports:?}");
    assert_eq!(alpha_reports[0]["verdict"], "fail");
    assert_eq!(alpha_reports[0]["reason"], "first word");
}

/// A reviewer's token names its own seat and nothing else. Holding a
/// colleague's token buys nothing — not that seat, not any other.
#[test]
fn a_reviewers_token_reports_only_for_its_own_seat() {
    let world = World::configured();
    let build = builder(&world);
    let stash = world.root().join("alpha.token");
    // The first seat leaks its credential where the second can pick it up.
    let alpha = write_script(
        &world,
        "alpha.sh",
        &format!(
            "printf '%s\\n%s\\n' \"$SLOOP_SOCKET\" \"$SLOOP_TOKEN\" > {stash}\n\
             {sloop} --json verdict pass --reason 'acceptable' > alpha.out 2>&1 || true\n\
             exit 0\n",
            stash = shell_quote(&stash.to_string_lossy()),
            sloop = sloop_binary(),
        ),
    );
    // The second seat presents the first's token before its own.
    let beta = write_script(
        &world,
        "beta.sh",
        &format!(
            "SLOOP_SOCKET=$(sed -n 1p {stash})\n\
             SLOOP_TOKEN=$(sed -n 2p {stash})\n\
             export SLOOP_SOCKET SLOOP_TOKEN\n\
             {sloop} --json verdict pass --reason 'stolen credential' \
             > stolen.out 2> stolen.err || true\n\
             exit 0\n",
            stash = shell_quote(&stash.to_string_lossy()),
            sloop = sloop_binary(),
        ),
    );
    let gamma = reviewer(&world, "gamma", "pass", "acceptable", "medium");
    configure(
        &world,
        &three_seat_flow(3, "fail"),
        &[
            ("builder", &build),
            ("alpha", &alpha),
            ("beta", &beta),
            ("gamma", &gamma),
        ],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-seat-scoped-token.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the run with a stolen credential settles", || {
        settled(&world)
    });

    // The stolen token is not the credential the live socket was issued
    // against, so the daemon refuses it outright.
    let stolen = worktree_json(&world, "stolen.err");
    assert_eq!(stolen["ok"], false, "{stolen}");
    assert_eq!(stolen["error"]["code"], "unauthorized");

    // And it reported for nobody: seat 0 holds only alpha's own report, and
    // seat 1 — whose reviewer spent its turn on the theft — holds none.
    let reports = panel_reports(&world, &world.run_id(1));
    let seats: Vec<&Value> = reports.iter().map(|report| &report["reviewer"]).collect();
    assert_eq!(
        reports
            .iter()
            .filter(|report| report["reviewer"] == 0)
            .count(),
        1,
        "{seats:?}"
    );
    assert!(
        reports.iter().all(|report| report["reviewer"] != 1),
        "{seats:?}"
    );
    // A quorum of three cannot be met with a seat unreported, so the theft
    // cost the run exactly what silence would have.
    let review = shown_stage(&world, &world.run_id(1), "review");
    assert_eq!(review["state"], "failed");
    assert_eq!(review["reason"], "panel: 2 of 3 reviewers passed, quorum 3");
}

/// A reviewer's token dies with its turn. Once the seat is done, the same
/// secret buys nothing in the stage that follows it or in any later run — the
/// socket is bound to a run and validates exactly one live credential at a
/// time.
#[test]
fn a_reviewers_token_cannot_report_for_a_later_stage_or_another_run() {
    let world = World::configured();
    let build = builder(&world);
    let stash = world.root().join("alpha.token");
    // Seat 0 stashes its credential the first time it runs, and thereafter
    // always tries the stashed one before its own. In the first run that
    // stashed token *is* its own and the report lands; in the second it is a
    // credential from a run that has already settled.
    let alpha = write_script(
        &world,
        "alpha.sh",
        &format!(
            "if [ ! -f {stash} ]; then printf '%s' \"$SLOOP_TOKEN\" > {stash}; fi\n\
             SLOOP_TOKEN=$(cat {stash}) {sloop} --json verdict pass --reason 'stashed credential' \
             > stashed.out 2> stashed.err || true\n\
             {sloop} --json verdict pass --reason 'own credential' > own.out 2>&1 || true\n\
             exit 0\n",
            stash = shell_quote(&stash.to_string_lossy()),
            sloop = sloop_binary(),
        ),
    );
    let beta = reviewer(&world, "beta", "pass", "acceptable", "medium");
    // A `reported` stage after the panel, presenting the reviewer's token
    // before its own. One script proves both halves of the stage boundary: the
    // superseded credential is refused, and the stage's real one still works.
    let sign_off = write_script(
        &world,
        "sign-off.sh",
        &format!(
            "MINE=$SLOOP_TOKEN\n\
             SLOOP_TOKEN=$(cat {stash}) {sloop} --json verdict fail --reason 'stale' \
             > stale.out 2> stale.err || true\n\
             SLOOP_TOKEN=$MINE {sloop} --json verdict pass --reason 'signed off' \
             > signed.out 2>&1 || true\n\
             exit 0\n",
            stash = shell_quote(&stash.to_string_lossy()),
            sloop = sloop_binary(),
        ),
    );
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n\
             - name: review\n  \
               action: {{ exec: ['true'] }}\n  \
               result_check:\n    \
                 panel:\n      \
                   prompt: prompts/panel.md\n      \
                   reviewers: [{{ target: alpha }}, {{ target: beta }}]\n      \
                   require: {{ quorum: 2 }}\n\
             - name: sign_off\n  \
               action: {{ exec: [\"sh\", {script}] }}\n  \
               result_check: reported\n",
            script = serde_json::to_string(&sign_off.to_string_lossy()).expect("quote path"),
        ),
        &[("builder", &build), ("alpha", &alpha), ("beta", &beta)],
    );
    world.commit_all("initial");
    world.start_daemon();
    let first = post(&world, "panel-stale-token.md");
    assert!(world.sloop(&["run", &first]).status.success());

    wait_until_slow("the first run finishes its sign-off", || {
        world.run_worktree(1).join("signed.out").is_file()
    });

    // A later stage of the very same run cannot use the reviewer's token: the
    // credential the socket validates against was replaced when this stage
    // minted its own.
    let stale = worktree_json(&world, "stale.err");
    assert_eq!(stale["ok"], false, "{stale}");
    assert_eq!(stale["error"]["code"], "unauthorized");
    // Its own token still works, so the refusal is about the credential and
    // not about the socket having been shut.
    let signed = worktree_json(&world, "signed.out");
    assert_eq!(signed["ok"], true, "{signed}");
    assert_eq!(signed["data"]["verdict"]["stage"], "sign_off");

    // A second ticket through the same flow. Seat 0 now replays the settled
    // run's credential against a socket that never issued it.
    let second = post(&world, "panel-foreign-token.md");
    assert!(world.sloop(&["run", &second]).status.success());
    wait_until_slow("the second run finishes its panel", || {
        panel_reports(&world, &world.run_id(2)).len() == 2
    });

    let foreign: Value = serde_json::from_str(
        &fs::read_to_string(world.run_worktree(2).join("stashed.err")).expect("read stashed.err"),
    )
    .expect("stashed.err is JSON");
    assert_eq!(foreign["ok"], false, "{foreign}");
    assert_eq!(foreign["error"]["code"], "unauthorized");

    // The foreign token reported for nothing. Each run's seat 0 holds exactly
    // the one report its own live credential authorised.
    for (position, reason) in [(1, "stashed credential"), (2, "own credential")] {
        let reports = panel_reports(&world, &world.run_id(position));
        let seat_zero: Vec<&Value> = reports
            .iter()
            .filter(|report| report["reviewer"] == 0)
            .collect();
        assert_eq!(seat_zero.len(), 1, "run {position}: {reports:?}");
        assert_eq!(seat_zero[0]["reason"], reason, "run {position}");
    }
}

/// A seat's brief is its own credential read back: the stage it was minted
/// for, and the obligation that stage puts on a reviewer. Sloop's own panel
/// prompt tells a reviewer to change nothing, so a brief that told it to commit
/// would have the daemon contradicting itself in two strings it authored.
///
/// It is also the whole of what a seat learns: two seats of the same panel read
/// the identical brief, so nothing in it identifies a colleague.
#[test]
fn a_panel_seats_brief_names_its_stage_and_hides_the_other_seats() {
    let world = World::configured();
    let build = builder(&world);
    let briefing = |name: &str| {
        write_script(
            &world,
            &format!("{name}.sh"),
            &format!(
                "{sloop} --json brief > {name}-brief.json 2>&1 || true\n\
                 {sloop} --json verdict pass --reason 'acceptable' >/dev/null 2>&1 || true\n\
                 exit 0\n",
                sloop = sloop_binary(),
            ),
        )
    };
    let alpha = briefing("alpha");
    let beta = briefing("beta");
    configure(
        &world,
        "- { name: build, action: agent, result_check: { exec: ['true'] } }\n\
         - name: review\n  \
           action: { exec: ['true'] }\n  \
           result_check:\n    \
             panel:\n      \
               prompt: prompts/panel.md\n      \
               reviewers: [{ target: alpha }, { target: beta }]\n      \
               require: { quorum: 2 }\n\
         - { name: merge, action: { builtin: merge }, result_check: none }\n",
        &[("builder", &build), ("alpha", &alpha), ("beta", &beta)],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-brief.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the briefed panel merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let alpha_brief = worktree_json(&world, "alpha-brief.json");
    assert_eq!(alpha_brief["ok"], true, "{alpha_brief}");
    // The stage comes from the credential, not from whatever the driver is
    // running: a seat's brief names the stage its token was minted for.
    assert_eq!(alpha_brief["data"]["stage"]["name"], "review");
    assert_eq!(alpha_brief["data"]["stage"]["attempt"], 1);
    assert_eq!(alpha_brief["data"]["stage"]["result_check"], "panel");
    let obligation = alpha_brief["data"]["definition_of_done"][0]
        .as_str()
        .expect("obligation text")
        .to_lowercase();
    assert!(
        obligation.contains("reported verdict"),
        "reviewer obligation: {obligation}"
    );
    assert!(
        !obligation.contains("commit"),
        "a panel reviewer was told to commit: {obligation}"
    );

    // Seat 0 and seat 1 read the same words. There is no seat index, no
    // colleague's target, and no other seat's report in a brief — so holding
    // one seat's credential reveals nothing about the panel around it. Only
    // the envelope's echoed request id differs, which is per-call and says
    // nothing about who asked.
    assert_eq!(
        alpha_brief["data"],
        worktree_json(&world, "beta-brief.json")["data"]
    );
}

/// A backward edge re-runs the panel, and the second round's reports belong to
/// the second round. Keying them by attempt is what keeps a converged loop
/// from being decided by the votes that sent it back.
#[test]
fn reports_land_on_the_right_attempt_after_a_return_to_re_run() {
    let world = World::configured();
    let build = builder(&world);
    // Both reviewers reject the first round and accept the second, counting
    // through a file outside the worktree so the tally survives the re-run.
    let turncoat = |name: &str| {
        let counter = world.root().join(format!("{name}.count"));
        fs::write(&counter, b"").expect("create counter");
        write_script(
            &world,
            &format!("{name}.sh"),
            &format!(
                "printf x >> {counter}\nrounds=$(wc -c < {counter} | tr -d ' ')\n\
                 if [ \"$rounds\" -le 1 ]; then\n  \
                   {sloop} --json verdict fail --reason 'round one' >/dev/null 2>&1 || true\n\
                 else\n  \
                   {sloop} --json verdict pass --reason 'round two' >/dev/null 2>&1 || true\n\
                 fi\nexit 0\n",
                counter = shell_quote(&counter.to_string_lossy()),
                sloop = sloop_binary(),
            ),
        )
    };
    let alpha = turncoat("alpha");
    let beta = turncoat("beta");
    configure(
        &world,
        "- { name: build, action: agent, result_check: { exec: ['true'] } }\n\
         - name: review\n  \
           action: { exec: ['true'] }\n  \
           result_check:\n    \
             panel:\n      \
               prompt: prompts/panel.md\n      \
               reviewers: [{ target: alpha }, { target: beta }]\n      \
               require: { quorum: 2 }\n  \
           fail_action: { return_to: build, attempts: 1 }\n\
         - { name: merge, action: { builtin: merge }, result_check: none }\n",
        &[("builder", &build), ("alpha", &alpha), ("beta", &beta)],
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "panel-return-to.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the re-reviewed run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // Four rows: two seats twice over, each round on its own attempt. The
    // first round's rejections are still there — a re-run supersedes a verdict
    // in the walk, it does not erase the evidence behind it.
    let run = world.run_id(1);
    let reports = panel_reports(&world, &run);
    let mut keyed: Vec<(u64, u64, String)> = reports
        .iter()
        .map(|report| {
            (
                report["attempt"].as_u64().expect("attempt"),
                report["reviewer"].as_u64().expect("reviewer"),
                report["verdict"].as_str().expect("verdict").to_owned(),
            )
        })
        .collect();
    keyed.sort();
    assert_eq!(
        keyed,
        vec![
            (1, 0, "fail".to_owned()),
            (1, 1, "fail".to_owned()),
            (2, 0, "pass".to_owned()),
            (2, 1, "pass".to_owned()),
        ]
    );

    // And the walk read them per attempt: the first execution of `review`
    // failed, the second passed.
    let executions: Vec<(u64, String, String)> = world.show_snapshot(&run)["stages"]
        .as_array()
        .expect("stages array")
        .iter()
        .filter(|row| row["stage"] == "review")
        .map(|row| {
            (
                row["attempt"].as_u64().unwrap_or(0),
                row["state"].as_str().unwrap_or("?").to_owned(),
                row["reason"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    assert_eq!(
        executions,
        vec![
            (
                1,
                "failed".to_owned(),
                "panel: 0 of 2 reviewers passed, quorum 2".to_owned()
            ),
            (
                2,
                "passed".to_owned(),
                "panel: 2 of 2 reviewers passed, quorum 2".to_owned()
            ),
        ]
    );
}
