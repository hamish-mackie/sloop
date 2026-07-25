//! Upgrading in place, from the operator's side of the socket.
//!
//! Schema migrations are unit-tested against the tables they rewrite. What
//! those tests cannot show is that the daemon still *works* afterwards, which
//! is the only property an operator cares about: they stopped a daemon holding
//! pending work, installed a new binary, and started it again.

mod support;

use support::{FakeAgent, World, revert_trigger_rename, wait_until};

fn status(world: &World) -> serde_json::Value {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"].clone()
}

fn post_manual(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
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

/// Puts the daemon's state back to schema 15 — triggers still called
/// activations, ids still `A<ordinal>` — so the next start is a genuine
/// in-place upgrade rather than a fresh database.
fn plant_activation_shape(world: &World) {
    revert_trigger_rename(world);
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .pragma_update(None, "user_version", 15)
        .expect("put the schema version back");
}

/// The queued work an operator leaves behind is the whole reason the rename
/// needed a migration rather than a `reindex`. Nothing in the repository
/// records that a run was asked for, so a trigger that did not survive the
/// upgrade is a run that silently never happens.
#[test]
fn queued_work_planted_before_the_rename_still_dispatches_after_it() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;

    // Pausing is what makes the trigger sit still: it is queued and eligible,
    // and the only reason it has not fired is the gate.
    let ticket = post_manual(&world, "pending-work.md");
    assert!(world.sloop(&["pause"]).status.success());
    assert!(world.sloop(&["run", &ticket]).status.success());
    let queued = status(&world)["queued_triggers"].clone();
    assert_eq!(queued.as_array().unwrap().len(), 1);
    assert_eq!(queued[0]["ticket"], ticket.as_str());
    let id_before = queued[0]["id"].as_str().unwrap().to_owned();
    assert!(id_before.starts_with("TR"), "{id_before}");

    world.kill_daemon(daemon_pid);
    plant_activation_shape(&world);
    world.start_daemon();

    // The same trigger came across, under a rewritten id.
    let queued = status(&world)["queued_triggers"].clone();
    assert_eq!(queued.as_array().unwrap().len(), 1);
    assert_eq!(queued[0]["ticket"], ticket.as_str());
    assert_eq!(queued[0]["id"], id_before.as_str());

    // And it is still live demand, not an inert row: releasing the gate
    // dispatches it without anyone asking a second time.
    assert!(world.sloop(&["resume"]).status.success());
    wait_until("the migrated trigger dispatches and merges", || {
        status(&world)["tickets"]["merged"] == 1
    });
    assert!(
        status(&world)["queued_triggers"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

/// `queued_activations` is the field's pre-rename name. It is deprecated in
/// 0.4.0 and removed in 0.5.0, and until then it has to carry exactly what
/// replaced it — a dashboard reading the old name must not see a different
/// queue from one reading the new name.
#[test]
fn the_dashboard_emits_the_deprecated_queued_activations_alias() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    world.start_daemon();

    let ticket = post_manual(&world, "aliased.md");
    assert!(world.sloop(&["pause"]).status.success());
    assert!(world.sloop(&["run", &ticket]).status.success());

    let snapshot = status(&world);
    assert_eq!(snapshot["queued_triggers"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["queued_activations"], snapshot["queued_triggers"]);

    // `show` builds the dashboard from the same status fields, so the alias
    // has to reach it too.
    let dashboard = World::json_stdout(&world.sloop(&["show"]))["data"].clone();
    assert_eq!(dashboard["kind"], "dashboard");
    assert_eq!(
        dashboard["queued_activations"],
        dashboard["queued_triggers"]
    );
}
